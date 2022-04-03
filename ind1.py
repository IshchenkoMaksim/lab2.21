#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Для своего варианта лабораторной работы 2.17 необходимо
реализовать хранение данных в базе данных SQLite3.
"""

import argparse
import sqlite3
import typing as t
from pathlib import Path


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


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Создать таблицу с информацией о направлениях маршрутов.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS routes (
        route_id INTEGER PRIMARY KEY AUTOINCREMENT,
        destination TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о маршрутах
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS way (
        way_id INTEGER PRIMARY KEY AUTOINCREMENT,
        number INTEGER NOT NULL,
        route_id INTEGER NOT NULL,
        time TEXT NOT NULL,
        FOREIGN KEY(route_id) REFERENCES routes(route_id)
        )
        """
    )
    conn.close()


def add_route(
        database_path: Path,
        number: int,
        destination: str,
        time: str
) -> None:
    """
    Добавить маршрут в базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Получить идентификатор пути в базе данных.
    # Если такой записи нет, то добавить информацию о новом маршруте.
    cursor.execute(
        """
        SELECT route_id FROM routes WHERE destination = ?
        """,
        (destination,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO routes (destination) VALUES (?)
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
        VALUES (?, ?, ?)
        """,
        (number, route_id, time)
    )
    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать все маршруты
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT way.number, routes.destination, way.time
        FROM way
        INNER JOIN routes ON routes.route_id = way.route_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "number": row[0],
            "destination": row[1],
            "time": row[2],
        }
        for row in rows
    ]


def select_by_time(
        database_path: Path, time1: str
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать все маршруты после заданного времени
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT way.number, routes.destination, way.time
        FROM way
        INNER JOIN routes ON routes.route_id = way.route_id
        WHERE strftime('%H:%M', way.time) > strftime('%H:%M', ?)
        """,
        (time1,)
    )
    # datetime.
    rows = cursor.fetchall()
    conn.close()
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
    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)
    # Добавить маршрут.
    if args.command == "add":
        add_route(db_path, args.number, args.destination, args.time)
    # Отобразить все маршруты.
    elif args.command == "display":
        display_routes(select_all(db_path))
    # Выбрать требуемые маршруты.
    elif args.command == "select":
        display_routes(select_by_time(db_path, args.time))
    pass


if __name__ == '__main__':
    main()
