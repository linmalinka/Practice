import os
import sys
import requests
from enum import Enum
import kafka
from kafka import TopicPartition
from datetime import timedelta, datetime


# project root
PROJECT_MAIN_FOLDER = os.path.realpath(sys.argv[0]).split('/')[:-4]
# migrations package
MIGRATIONS_PACKAGE = '/'.join(PROJECT_MAIN_FOLDER)
sys.path.insert(0, MIGRATIONS_PACKAGE)
# PATH
print(sys.path)
from migrations.config.configuration import get_config


CM_USER = "admin"
CM_PASSWORD = "admin"
CM_HOST = "localhost:7180" # cloudera_host:7180
CM_CLUSTER = f"Cluster%201%20-%20CDH4"
BASE_URL = "http://{host}".format(host=CM_HOST) #http://localhost:7180


class ROLLBACK(Enum):
    NOT_REQUIRED = "not_required",
    REQUIRED = "required"


SERVICES= {

    "hue1": {
        "search_query": "SELECT max(date) from test",
        "rollback_strategy": ROLLBACK.NOT_REQUIRED,
        "rollback_value": "00:02:00", # сдвиг на 2 минуты назад
        "topic": "test_peter",
        "group_id": "group_1"
    
    },

    "spark1": {
        "search_query": "SELECT max(date) from test",
        "rollback_strategy": ROLLBACK.REQUIRED,
        "rollback_value": "00:05:00", 
        "topic": "test_1",
        "group_id": "group_2"

    },

    "kafka1": {
        "search_query": "SELECT max(date) from test",
        "rollback_strategy": ROLLBACK.NOT_REQUIRED,
        "rollback_value": "02:00:00",
        "topic": "test_3",
        "group_id": "group_3"

    }
}


def load_info_from_cloudera(service_name: str) -> dict:
    """Тянет инфо сервиса-стриминга из Cloudera Manager по ключу из SERVICES. Кидает Exception при коде != 200."""

    r = requests.get(f"{BASE_URL}/api/v1/clusters/{CM_CLUSTER}/services/{service_name}",
                         auth=(f"{CM_USER}", f"{CM_PASSWORD}"),)

    if r.status_code != 200:
        print("Cannot load info from Cloudera: {code} - {details}".format(code=r.status_code, details=r.text))
        raise Exception("Cannot load info from Cloudera Manager")

    return r.json() # --> cm_info


def is_streaming_running(info: dict) -> bool:
    """Определяет, жив ли стриминг: True только если serviceState == STARTED и healthSummary == GOOD."""
    """Если STARTED, то не факт, что стриминг работает, поэтому дополнительно\
        проверяем состояние через healthSummary, если GOOD, то все работает и ничего делать не нужно"""
    
    _ = "Streaming: " + info.get("displayName") + ". Status: {status}"
    
    if (info.get("serviceState") == "STARTED") and (info.get("healthSummary") == "GOOD"):
        print(_.format(status="Good"))
        return True      
    
    else:
        print(_.format(status="Bad"))
        print("Streaming needs a restart")
        return False


def is_rollback_required(service_name: str) -> bool:
    """Проверяет по реестру SERVICES, нужен ли откат стриминга\
          (rollback_strategy == ROLLBACK.REQUIRED)."""
    
    if SERVICES.get(service_name).get("rollback_strategy") == ROLLBACK.REQUIRED:
        return True
    else:
        print("Rollback is not required")
        return False


def define_rollback_value(service_name: str) -> str:
    """Вычисляет точку отката запросом search_query в ClickHouse. Пока возвращает время offset для кафки. Кидает Exception при коде != 200."""

    # вытаскиваем информацию о времени последней записи данных из КХ-таблицы,
    # после чего из реестра SERVICES вытаскиваем дельту смещения
    # вычитаем время смещения из времени последней записи

    # вытаскиваем из SERVERS запрос в кх
    r = requests.get(
        url="http://{host}:{port}/?user={user}&password={password}".format(**get_config().CLICKHOUSE_CONNECTION_INFO),
        params={"query": SERVICES.get(service_name).get("search_query")}) 
    
    if r.status_code != 200:
        print("Cannot load rollback time from ClickHouse: {code}. {details}".format(code=r.status_code, details=r.text))
        raise Exception("Cannot load rollback time from ClickHouse")
    
    last_time = r.text.strip()
    rollback_delta = SERVICES.get(service_name).get("rollback_value") # доcтаем значение дельты из SERVICES по имени сервиса

    print("Date from KH: " + last_time)
    print("Rollback value: " + rollback_delta)

    hour, minute, second = map(int, rollback_delta.split(':'))
    # вычитаем и получаем нужное значение сдвига
    # использовала встроенную библиотеку datetime
    rollback_time = datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S') - timedelta(hours=hour,minutes=minute,seconds=second)

    # в rollback_time  лежит финальная дата для отката в кафке (оффсет)

    print("Rollback time: " + datetime.strftime(rollback_time, '%Y-%m-%d %H:%M:%S'))
    return rollback_time


def apply_rollback_to_kafka(service_name: str, rollback_time: datetime.datetime) -> None:
    """Сдвигаем Kafka-оффсеты потребителя на точку отката"""
    
    consumer = kafka.KafkaConsumer(
        bootstrap_servers=get_config().KAFKA_BOOTSTRAP_SERVERS,
        group_id= SERVICES.get(service_name).get("group_id")
    )

    topic = SERVICES.get(service_name).get("topic")
    t_partitions = [TopicPartition(topic, partition)
                    for partition in consumer.partitions_for_topic(topic)]
    
    consumer.assign(t_partitions)
    timestamp_ms = int(rollback_time.timestamp() * 1000)

    # проходим по всем партициям и меняем значение offset
    for partition in t_partitions:
        offsets = consumer.offsets_for_times({partition: timestamp_ms}) # вычисляем смещение offset по времени в мс
        offset_info = offsets.get(partition) # вытаскиваем инфо об офсете партиции

        if offset_info is None:
            print(f'topic: {partition[0]}, partition: {partition[1]}')
            print('Current offset: offset not found')
            continue
        

        print(f'topic: {partition[0]}, partition: {partition[1]}')
        print(f'Current offset: {consumer.position(partition)}')
        print(f'Rollback offset: {offset_info.offset}')

        # смещаем офсет
        consumer.seek(partition, offset_info.offset)

        print('===')
        print(f'New offset: {consumer.position(partition)}' )
        print('===')

    consumer.commit() # сохраняем изменения
    consumer.close()


      
def  restart_streaming(service_name: str) -> bool:
    """ Осуществляем перезапуск сервиса командой перезапуска. Кидаем Exeption, если статус ответа не 200"""

    r = requests.post(
        f"http://localhost:7180/api/v1/clusters/{CM_CLUSTER}/services/{service_name}/commands/restart", 
                      auth=(f"{CM_USER}", f"{CM_PASSWORD}")
                      )
    
    if r.status_code != 200:
        print("Service restart failed")
        raise Exception("Failed retart")

    print("Service restart")
    return True


if __name__ == '__main__':

    for service in SERVICES.keys():
        print(f"Service name: {service}")
        cm_info = load_info_from_cloudera(service) # --> json
        status = is_streaming_running(cm_info)
        if not status:
            rollback = is_rollback_required(service)
            if rollback:
                value = define_rollback_value(service)
            
                apply_rollback_to_kafka(service, value)

            restart_streaming(service)
