from common_constants import constants
import requests
from time import sleep
from datetime import date, timedelta
import pickle

ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/yandex_metrika/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


class InternalMetrikaServerError(constants.PySeaError): pass
class YandexMetrikaError(constants.PySeaError): pass
class LimitOfRetryError(constants.PySeaError): pass


def limit_by(nlim):  # конструктор декоратора (L залипает в замыкании)
    """
    Декоратор для использования постраничной выборки в вызовах API Яндекс Метрики
    https://yandex.ru/dev/metrika/doc/api2/api_v1/data.html

    :param nlim: не более 100 000 объектов за один запрос. (для метода get)
    :return:
    """
    def deco_limit(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            result = []
            self.limit_by = abs(nlim) if abs(nlim) <= 100000 else 100000

            data = f(self, *argp, **argn)
            result.extend(data[0])
            total_rows = data[1] - self.limit_by  # первая порция данных уже получена

            while self.offset < total_rows:
                self.offset += self.limit_by
                data = f(self, *argp, **argn)
                result.extend(data[0])

            self.offset = 1  # не забываем вернуть пагенатор в исходное состояние для следующих вызовов
            return result
        return constructed_function
    return deco_limit


def connection_attempts(n=12, t=10):  # конструктор декоратора (N,T залипает в замыкании)
    """
    Декоратор задает n попыток для соединения с сервером в случае ряда исключений
    с задержкой t*2^i секунд

    :param n: количество попыток соединения с сервером [1, 15]
    :param t: количество секунд задержки на первой попытке попытке (на i'ом шаге t*2^i)
    :return:
    """
    def deco_connect(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(*argp, **argn):  # конструируемая функция
            retry_flag, pause_seconds = n, t
            try_number = 0

            if retry_flag < 0 or retry_flag > 15:
                retry_flag = 8
            if pause_seconds < 1 or pause_seconds > 30:
                pause_seconds = 10

            while True:
                try:
                    result = f(*argp, **argn)
                    # Обработка ошибки, если не удалось соединиться с сервером
                except (ConnectionError,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.ChunkedEncodingError,
                        InternalMetrikaServerError,) as err:
                    logger.error(f"Ошибка соединения с сервером {err}. Осталось попыток {retry_flag - try_number}")
                    if try_number >= retry_flag:
                        raise LimitOfRetryError
                    sleep(pause_seconds * 2 ** try_number)
                    try_number += 1
                    continue
                else:
                    return result

            return None
        return constructed_function
    return deco_connect


def dump_to(prefix, d=False):  # конструктор декоратора (n залипает в замыкании)
    """
    Декоратор для кеширования возврата функции.
    Применим к методам класса, в котором объявлены:
    self.directory - ссылка на каталог
    self.dump_file_prefix - файловый префикс
    self.cache - True - кеширование требуется / False
    На вход принимает префикс, который идентифицирует декорируемую функцию

    Кеш хранится в сериализованных файлах с помощью pickle

    :param prefix: идентифицирует декорируемую кешируемую функцию
    :param d: явно указанная дата в self.current_date или False для сегодняшней даты (для формирования имени файла)
    :return:
    """
    def deco_dump(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            if 'dump_parts_flag' in self.__dict__:
                dump_file_prefix = f"{self.dump_file_prefix}_p{self.dump_parts_flag['part_num']}"
            else:
                dump_file_prefix = self.dump_file_prefix

            if not d:
                file_out = "{}/{}_{}_{}.pickle".format(self.directory, dump_file_prefix, prefix,
                                                       date.today()).replace("//", "/")
            else:
                file_out = "{}/{}_{}_{}.pickle".format(self.directory, dump_file_prefix, prefix,
                                                       self.current_date).replace("//", "/")
            read_data = ""

            if self.cache:  # если кеширование требуется
                try:  # пробуем прочитать из файла
                    with open(file_out, "rb") as file:
                        read_data = pickle.load(file)
                except Exception as err:
                    logger.debug(f"{err}\n Cache file {file_out} is empty, getting fresh...")

            if not read_data:  # если не получилось то получаем данные прямым вызовом функции
                read_data = f(self, *argp, **argn)
                if 'dump_parts_flag' in self.__dict__:
                    self.dump_parts_flag['len'] = len(read_data)

                with open(file_out, "wb") as file:  # записываем результат в файл
                    if 'dump_parts_flag' in self.__dict__:
                        pickle.dump(read_data[-self.dump_parts_flag['len']:], file, pickle.HIGHEST_PROTOCOL)
                    else:
                        pickle.dump(read_data, file, pickle.HIGHEST_PROTOCOL)
            return read_data
        return constructed_function
    return deco_dump


class YandexMetrikaBase:
    def __init__(self, directory: str = "./", dump_file_prefix: str = "fooooo", cache: bool = True) -> None:

        self.service = {
            "table": 'https://api-metrika.yandex.net/stat/v1/data',
            "drilldown": 'https://api-metrika.yandex.net/stat/v1/data/drilldown',
            "bytime": 'https://api-metrika.yandex.net/stat/v1/data/bytime',
            "comparison": 'https://api-metrika.yandex.net/stat/v1/data/comparison',
            "comparison-drill": 'https://api-metrika.yandex.net/stat/v1/data/comparison/drilldown',
        }
        self.counter = ENVI['PYSEA_YM_COUNTERID']
        self.headers = {"Authorization": f"OAuth {ENVI['PYSEA_METRIKA_TOKEN']}"}
        self.begin_date = date.today() - timedelta(7)
        self.end_date = date.today() - timedelta(1)
        self.accuracy = "full"

        # переменные настраивающие кеширование запросов к API
        self.directory = directory
        self.dump_file_prefix = dump_file_prefix
        self.cache = cache

        # постраничная выборка
        self.limit_by = 100000
        self.offset = 1

    def set_data_range(self, begin: str, end: str = ""):  # -> YandexMetrikaBase:
        """
        Устанавливает период для запроса отчета Yandex Metrika
        Начальная и конечная даты должны быть заданы в формате ISO 8601 YYYY-MM-DD

        :param begin: YYYY-MM-DD
        :param end: YYYY-MM-DD
        :return:
        """
        if not end:
            end = begin

        self.begin_date = begin if type(begin) is date else date.fromisoformat(begin)
        self.end_date = end if type(end) is date else date.fromisoformat(end)
        return self

    def set_accuracy_level(self, level: str = "low"):  # -> YandexMetrikaBase:
        """
        https://yandex.ru/dev/metrika/doc/api2/api_v1/sampling.html

        :param level: low/medium/high/full or (0,1]
        :return: self
        """
        self.accuracy = level
        return self

    def cache_enabled(self) -> None:
        self.cache = True

    def cache_disabled(self) -> None:
        self.cache = False

    def send_request(self, body: dict, srv_type: str = "table") -> requests.models.Response:
        """
        Выполняет непосредственно запрос к серверу API
        Принимает на входе сформированное тело запроса и тип запроса

        :param body: тело запроса к API Яндекс Метрика
        :param srv_type: тип запроса (метка URL запроса)
        :return: возврящает полный ответ сервера
        """

        params = {
            "id": self.counter,
            "date1": self.begin_date,
            "date2": self.end_date,
            "accuracy": self.accuracy,
            "limit": self.limit_by,  # может перегружаться при следующем update
            "offset": self.offset,  # может перегружаться при следующем update
        }
        params.update(body)

        # Выполнение запроса
        result = requests.get(self.service[srv_type], params=params, headers=self.headers)
        json_result = result.json()
        # Обработка запроса
        # https://yandex.ru/dev/metrika/doc/api2/api_v1/concept/errors.html
        if result.status_code != 200 or json_result.get("errors", False):
            logger.error(f"Произошла ошибка при обращении к серверу API Метрики. {json_result['errors']}\n")
            logger.error(f"Request params: {params}")
            if result.status_code in {
                503,  # backend_error
                429,  # quota_requests_by_ip
                429,  # quota_parallel_requests
                504,  # timeout
            }:
                raise InternalMetrikaServerError
            else:
                raise YandexMetrikaError
        else:
            logger.info(f"YM OK: sample_share {json_result['sample_share']} "
                        f"// {len(json_result['data'])} of {json_result['total_rows']} lines")

        return result

    @dump_to("example_stat")  # кешируем в файл
    @limit_by(5000)
    @connection_attempts()  # делает доп попытки в случае возникновения ConnectionError
    def example(self):
        body = {
            "metrics": "ym:s:users",
            "dimensions": "ym:s:date,ym:s:lastsignUTMCampaign",
        }

        body.update({"limit": self.limit_by, "offset": self.offset})
        result = self.send_request(body, srv_type="table").json()
        return result['data'], result['total_rows']


if __name__ == '__main__':
    ym = YandexMetrikaBase()
    res = ym.example()
    print(res)
