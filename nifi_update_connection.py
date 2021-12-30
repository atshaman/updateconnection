# -*-coding: utf-8 -*-
""" Программа для изменения FlowFileExpiration в nifi-схеме
Для работы необходимы модули click, lxml, beautifulsoup4 """
import click
import gzip
import sys
import datetime
from bs4 import BeautifulSoup

timeunits = ("sec", "seconds", "min", "minutes", "hour", "hours", "day", "days", "week", "weeks")
defaulttime = ['0 sec']


@click.command(help="Программа для изменения FlowFileExpiration в NiFi")
@click.option("-i", "--input", help="Путь к flow.xml.gz", prompt=True, type=click.File('rb'), default="flow.xml.gz")
@click.option("-o", "--output", help="Путь к файлу с внесенными изменениями", type=click.File('wb'))
@click.option("-f", "--force/--no-force", help="Перезаписывать установленные параметры", default=False)
@click.option("-d", "--dryrun/--no-dryrun", help="Показать изменения", default=False)
@click.option("-v", "--verbose", default=0, count=True, help="Детализация вывода")
@click.option("-e", "--expire", help="Интервал удаления", required=True, nargs=2, type=(int, str))
@click.option("-c", "--compress/--no-compress", help='Сжимать файл', default=False)
def main(input, output, force, verbose, dryrun, expire, compress):
    if expire[1] not in timeunits:
        # Проверка корректности задания единиц времени. Использованы НЕ все возможные варианты
        sys.exit(f"Единицы измерения не опознана! Возможные варианты: {timeunits}")
    if output == input:
        sys.exit('Работа in-place запрещена!')
    # Определяем данные для статистики
    ts = datetime.datetime.now()
    counter, total = 0, 0
    if not output:
        # В случае отсутствия вывода пишем в консоль
        output = sys.stdout.buffer
    xml = gzip.decompress(input.read())
    soup = BeautifulSoup(xml, 'xml')
    for i in soup.find_all('flowFileExpiration'):
        total += 1
        if force or i.contents == defaulttime:
            # Если задан параметр force меняем все значения, в противном случае только параметры по умолчанию
            # Если задан параметр dryrun выводим в консоль изменения, но не меняем данные в файле
            if verbose == 2:
                print(f"Соединение {i.parent.id.string} изменено")
            if not dryrun:
                i.string.replace_with(f'{expire[0]} {expire[1]}')
            counter += 1
    if compress:
        output.write(gzip.compress(soup.encode('utf-8')))
    else:
        output.write(soup.prettify().encode('utf-8'))
    if verbose:
        print()
        print(f'В конфигурации {total} связей, изменено {counter}')
    if verbose >= 1:
        print(f'Операция выполнена за {datetime.datetime.now() - ts}')


if __name__ == '__main__':
    main()
