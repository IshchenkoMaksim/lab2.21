#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Самостоятельно изучите работу с пакетом python-psycopg2 для работы с базами
данных PostgreSQL. Для своего варианта лабораторной работы 2.17 необходимо
реализовать возможность хранения данных в базе данных СУБД PostgreSQL.
"""

import argparse
import psycopg2
import typing as t
from pathlib import Path


def connect():
    conn = psycopg2.connect(
        user="postgres",
        password="12345",
        host="localhost",
        port="5432")

    return conn


def display_routes(way: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить список маршрутов.
    """
    if way:
        line = '+-{}-+-{}-+-{}-+'.format(
            '-' * 30,
            '-' * 4,
            '-' * 20
        )
        print(line)
        print(
            '| {:^30} | {:^4} | {:^20} |'.format(
                "Пункт назначения",
                "№",
                "Время"
            )
        )
        print(line)

        for route in way:
            print(
                '| {:<30} | {:>4} | {:<20} |'.format(
                    route.get('destination', ''),
                    route.get('number', ''),
                    route.get('time', '')
                )
            )
        print(line)

    else:
        print("Маршруты не найдены")


def create_db() -> None:
    """
    Создать базу данных.
    """
    # Создать таблицу с информацией о направлениях маршрутов.
    cursor = connect().cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS routes (
        route_id serial,
        PRIMARY KEY(route_id),
        destination TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о маршрутах
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS way (
        way_id serial,
        PRIMARY KEY(way_id),
        number INTEGER NOT NULL,
        route_id INTEGER NOT NULL,
        time TEXT NOT NULL,
        FOREIGN KEY(route_id) REFERENCES routes(route_id)
        )
        """
    )


def add_route(
        number: int,
        destination: str,
        time: str
) -> None:
    """
    Добавить маршрут в базу данных.
    """
    # Получить идентификатор пути в базе данных.
    # Если такой записи нет, то добавить информацию о новом маршруте.
    cursor = connect().cursor()
    cursor.execute(
        """
        SELECT route_id FROM routes WHERE destination = %s
        """,
        (destination,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO routes (destination) VALUES (%s)
            """,
            (destination,)
        )
        route_id = cursor.lastrowid
    else:
        route_id = row[0]

        # Добавить информацию о новом продукте.
    cursor.execute(
        """
        INSERT INTO way (number, route_id, time)
        VALUES (%s, %s, %s)
        """,
        (number, route_id, time)
    )
    connect().commit()


def select_all():
    """
    Выбрать все маршруты
    """
    cursor = connect().cursor()
    cursor.execute(
        """
        SELECT way.number, routes.destination, way.time
        FROM way
        INNER JOIN routes ON routes.route_id = way.route_id
        """
    )
    rows = cursor.fetchall()
    return [
        {
            "number": row[0],
            "destination": row[1],
            "time": row[2],
        }
        for row in rows
    ]


def select_by_time(time1) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать все маршруты после заданного времени
    """
    cursor = connect().cursor()
    cursor.execute(
        """
        SELECT way.number, routes.destination, way.time
        FROM way
        INNER JOIN routes ON routes.route_id = way.route_id
        WHERE strftime('%H:%M', way.time) > strftime('%H:%M', %s)
        """,
        (time1,)
    )
    rows = cursor.fetchall()
    return [
        {
            "number": row[0],
            "destination": row[1],
            "time": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.home() / "routes.db"),
        help="The database file name"
    )
    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("routes")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )
    subparsers = parser.add_subparsers(dest="command")
    # Создать субпарсер для добавления маршрута.
    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new route"
    )
    add.add_argument(
        "-n",
        "--number",
        action="store",
        type=int,
        help="The way's number"
    )
    add.add_argument(
        "-d",
        "--destination",
        action="store",
        required=True,
        help="The way's name"
    )
    add.add_argument(
        "-t",
        "--time",
        action="store",
        required=True,
        help="Start time(hh:mm)"
    )
    # Создать субпарсер для отображения всех маршрутов.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all routes"
    )
    # Создать субпарсер для выбора маршрута.
    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the routes"
    )
    select.add_argument(
        "-t",
        "--time",
        action="store",
        required=True,
        help="The required period"
    )
    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)
    create_db()
    # Добавить маршрут.
    if args.command == "add":
        add_route(args.number, args.destination, args.time)
    # Отобразить все маршруты.
    elif args.command == "display":
        display_routes(select_all())
    # Выбрать требуемые маршруты.
    elif args.command == "select":
        display_routes(select_by_time(args.time))
    pass


if __name__ == '__main__':
    main()
