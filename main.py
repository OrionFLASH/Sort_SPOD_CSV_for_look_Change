#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Программа для сортировки CSV файлов
Сортирует строки по заданным полям с сохранением исходного форматирования
Обеспечивает одинаковую сортировку для двух файлов по одинаковым полям
"""

# Импортируем стандартные библиотеки Python
import os                    # Для работы с операционной системой
import sys                   # Для системных параметров
import csv                   # Для работы с CSV файлами
import logging              # Для логирования операций
import argparse             # Для парсинга аргументов командной строки
from datetime import datetime  # Для работы с датами и временем
from pathlib import Path    # Для работы с путями файловой системы
from typing import List, Dict, Any, Union, Set  # Для типизации данных

# Импортируем сторонние библиотеки
import pandas as pd         # Для работы с данными (если доступен)
from dateutil import parser as date_parser  # Для парсинга дат


# =============================================================================
# КОНФИГУРАЦИЯ ПРОГРАММЫ
# =============================================================================

# Базовые пути
BASE_PATH = '/Users/orionflash/Desktop/MyProject/SORT_CSV_SPOD/WORK'
INPUT_SUBFOLDER = 'INPUT'
OUTPUT_SUBFOLDER = 'OUTPUT'
LOGS_SUBFOLDER = 'LOGS'

# Настройки логирования
LOG_FILENAME = 'csv_sorter'

# Имена входных файлов (без расширения .csv)
INPUT_FILES = ['REWARD (PROM) 2025-08-07 — копия(Клара)', 'REWARD (PROM) 2025-07-24 v1']

# Настройки сортировки
SORT_CONFIG = {
    'delimiter': ';',  # Разделитель CSV файла (точка с запятой для ваших файлов)
    'fields': [
        {
            'name': 'REWARD_CODE',      # Имя поля для сортировки
            'type': 'text',            # Тип поля: 'auto', 'text', 'number', 'date'
            'order': 'asc'             # Порядок сортировки: 'asc' (возрастание), 'desc' (убывание)
        }
#        {
#            'name': 'salary',          # Имя поля для сортировки
#            'type': 'number',          # Тип поля: 'auto', 'text', 'number', 'date'
#            'order': 'desc'            # Порядок сортировки: 'asc' (возрастание), 'desc' (убывание)
#        },
#        {
#            'name': 'hire_date',       # Имя поля для сортировки
#            'type': 'date',            # Тип поля: 'auto', 'text', 'number', 'date'
#            'order': 'asc'             # Порядок сортировки: 'asc' (возрастание), 'desc' (убывание)
#        }
    ],
    'order': 'asc'  # Общий порядок сортировки (если не указан для конкретного поля)
}

# Примеры конфигураций для разных случаев:

# Сортировка по одному полю (числовому) в порядке убывания
SORT_CONFIG_NUMERIC_DESC = {
    'delimiter': ',',
    'fields': [
        {
            'name': 'price',
            'type': 'number',
            'order': 'desc'
        }
    ],
    'order': 'desc'
}

# Сортировка по нескольким полям
SORT_CONFIG_MULTI_FIELD = {
    'delimiter': ';',
    'fields': [
        {
            'name': 'department',
            'type': 'text',
            'order': 'asc'
        },
        {
            'name': 'salary',
            'type': 'number',
            'order': 'desc'
        },
        {
            'name': 'hire_date',
            'type': 'date',
            'order': 'asc'
        }
    ],
    'order': 'asc'
}

# Сортировка по дате
SORT_CONFIG_DATE = {
    'delimiter': ',',
    'fields': [
        {
            'name': 'date',
            'type': 'date',
            'order': 'desc'
        }
    ],
    'order': 'desc'
}


# =============================================================================
# ОСНОВНОЙ КЛАСС ДЛЯ СОРТИРОВКИ CSV
# =============================================================================

class CSVSorter:
    """Класс для сортировки CSV файлов"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация сортировщика
        
        Args:
            config: Словарь с конфигурацией программы
        """
        # Сохраняем конфигурацию в экземпляре класса
        self.config = config
        
        # Настраиваем систему логирования
        self.setup_logging()
        
        # Получаем логгер для текущего класса
        self.logger = logging.getLogger(__name__)
        
        # Создаем необходимые директории для работы
        self.create_directories()
        
        # Определяем общие поля для сортировки между файлами
        self.common_sort_fields = self.get_common_sort_fields()
        
    def setup_logging(self):
        """Настройка системы логирования на двух уровнях: INFO и DEBUG"""
        
        # Создаем директорию для логов внутри рабочей папки
        log_dir = Path(self.config['base_path']) / self.config['logs_subfolder']
        log_dir.mkdir(parents=True, exist_ok=True)  # Создаем папку, если её нет
        
        # Формируем полный путь к файлу логов
        log_file = log_dir / f"{self.config['log_filename']}.log"
        
        # Создаем форматтер для логов с временными метками
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Создаем файловый хендлер для записи логов в файл (уровень DEBUG)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Записываем все логи в файл
        file_handler.setFormatter(formatter)  # Применяем форматирование
        
        # Создаем консольный хендлер для вывода в терминал (уровень INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # В консоль только важные сообщения
        console_handler.setFormatter(formatter)  # Применяем форматирование
        
        # Настраиваем корневой логгер для всей программы
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)  # Устанавливаем минимальный уровень
        root_logger.addHandler(file_handler)  # Добавляем файловый хендлер
        root_logger.addHandler(console_handler)  # Добавляем консольный хендлер
        
        # Записываем информацию о настройке логирования
        logging.info(f"Логирование настроено. Файл логов: {log_file}")
        
    def create_directories(self):
        """Создание необходимых директорий для работы программы"""
        
        # Получаем базовый путь из конфигурации
        base_path = Path(self.config['base_path'])
        
        # Формируем пути к папкам INPUT и OUTPUT
        input_dir = base_path / self.config['input_subfolder']
        output_dir = base_path / self.config['output_subfolder']
        
        # Создаем папки, если их нет (parents=True создает родительские папки)
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Записываем информацию о созданных директориях
        self.logger.info(f"Директории созданы: {input_dir}, {output_dir}")
        
    def get_common_sort_fields(self) -> List[Dict[str, Any]]:
        """
        Определяет общие поля для сортировки, которые присутствуют в обоих файлах
        
        Returns:
            Список конфигураций полей для сортировки
        """
        
        # Формируем путь к папке с входными файлами
        base_path = Path(self.config['base_path'])
        input_dir = base_path / self.config['input_subfolder']
        
        # Словарь для хранения заголовков каждого файла
        file_headers = {}
        
        # Проходим по всем файлам из конфигурации
        for filename in self.config['input_files']:
            # Формируем полный путь к CSV файлу
            input_file = input_dir / f"{filename}.csv"
            
            # Проверяем существование файла
            if input_file.exists():
                try:
                    # Открываем файл для чтения
                    with open(input_file, 'r', encoding='utf-8', newline='') as f:
                        # Создаем CSV reader с заданным разделителем
                        reader = csv.reader(f, delimiter=self.config['sort_config']['delimiter'])
                        # Читаем первую строку (заголовок)
                        header = next(reader)
                        # Сохраняем заголовки как множество для быстрого поиска
                        file_headers[filename] = set(header)
                        # Записываем отладочную информацию
                        self.logger.debug(f"Файл {filename}: заголовки {header}")
                        
                except Exception as e:
                    # В случае ошибки чтения файла
                    self.logger.error(f"Ошибка чтения заголовков файла {filename}: {str(e)}")
                    file_headers[filename] = set()  # Пустое множество
            else:
                # Если файл не существует
                file_headers[filename] = set()
                
        # Находим пересечение заголовков всех файлов (общие поля)
        if len(file_headers) < 2:
            # Если файлов меньше двух, возвращаем исходную конфигурацию
            self.logger.warning("Недостаточно файлов для определения общих полей")
            return self.config['sort_config']['fields']
            
        # Находим общие заголовки во всех файлах
        common_headers = set.intersection(*file_headers.values())
        self.logger.info(f"Общие заголовки во всех файлах: {sorted(common_headers)}")
        
        # Фильтруем поля сортировки, оставляя только те, что есть в обоих файлах
        common_sort_fields = []
        for field_config in self.config['sort_config']['fields']:
            field_name = field_config['name']  # Имя поля из конфигурации
            
            if field_name in common_headers:
                # Если поле есть в обоих файлах, добавляем его для сортировки
                common_sort_fields.append(field_config)
                self.logger.info(f"Поле '{field_name}' будет использоваться для сортировки")
            else:
                # Если поля нет в одном из файлов, пропускаем его
                self.logger.warning(f"Поле '{field_name}' отсутствует в одном или нескольких файлах и будет пропущено")
                
        # Проверяем, что остались поля для сортировки
        if not common_sort_fields:
            self.logger.error("Нет общих полей для сортировки!")
            return []
            
        # Записываем итоговый список полей для сортировки
        self.logger.info(f"Итоговые поля для сортировки: {[f['name'] for f in common_sort_fields]}")
        return common_sort_fields
        
    def detect_field_type(self, sample_values: List[str]) -> str:
        """
        Определение типа поля на основе образцов значений
        
        Args:
            sample_values: Список образцов значений из поля
            
        Returns:
            Тип поля: 'text', 'number', 'date'
        """
        
        # Если нет образцов, считаем поле текстовым
        if not sample_values:
            return 'text'
            
        # Убираем пустые значения и пробелы
        non_empty = [v.strip() for v in sample_values if v.strip()]
        if not non_empty:
            return 'text'
            
        # Проверяем, сколько значений можно преобразовать в числа
        number_count = 0
        for value in non_empty:
            try:
                # Пытаемся преобразовать в число, заменяя запятую на точку
                float(value.replace(',', '.'))
                number_count += 1
            except ValueError:
                # Если не получилось, пропускаем
                pass
                
        # Если больше 80% значений - числа, считаем поле числовым
        if number_count / len(non_empty) > 0.8:
            return 'number'
            
        # Проверяем, сколько значений можно преобразовать в даты
        date_count = 0
        for value in non_empty:
            try:
                # Пытаемся распарсить дату
                date_parser.parse(value)
                date_count += 1
            except (ValueError, TypeError):
                # Если не получилось, пропускаем
                pass
                
        # Если больше 80% значений - даты, считаем поле датой
        if date_count / len(non_empty) > 0.8:
            return 'date'
            
        # По умолчанию считаем поле текстовым
        return 'text'
        
    def sort_value(self, value: str, field_type: str) -> Union[str, float, datetime]:
        """
        Преобразование значения для корректной сортировки
        
        Args:
            value: Значение для преобразования
            field_type: Тип поля (text, number, date)
            
        Returns:
            Преобразованное значение для сортировки
        """
        
        # Обрабатываем пустые значения
        if not value or value.strip() == '':
            # Для текста возвращаем пустую строку, для остального - минимальное значение
            return '' if field_type == 'text' else float('-inf')
            
        # Убираем лишние пробелы
        value = value.strip()
        
        if field_type == 'number':
            try:
                # Преобразуем в число, заменяя запятую на точку
                return float(value.replace(',', '.'))
            except ValueError:
                # В случае ошибки возвращаем минимальное число
                return float('-inf')
                
        elif field_type == 'date':
            try:
                # Парсим дату
                return date_parser.parse(value)
            except (ValueError, TypeError):
                # В случае ошибки возвращаем минимальную дату
                return datetime.min
        else:
            # Для текста приводим к нижнему регистру для корректной сортировки
            return value.lower()
            
    def sort_csv_file(self, input_file: Path, output_file: Path, sort_config: Dict[str, Any]):
        """
        Сортировка CSV файла по заданным полям
        
        Args:
            input_file: Путь к входному файлу
            output_file: Путь к выходному файлу
            sort_config: Конфигурация сортировки
        """
        
        self.logger.info(f"Начинаю сортировку файла: {input_file}")
        
        try:
            # Открываем входной файл для чтения
            with open(input_file, 'r', encoding='utf-8', newline='') as f:
                # Создаем CSV reader с заданным разделителем
                reader = csv.reader(f, delimiter=sort_config['delimiter'])
                # Читаем все строки в список
                rows = list(reader)
                
            # Проверяем, что файл не пустой
            if not rows:
                self.logger.warning(f"Файл {input_file} пуст")
                return
                
            # Разделяем заголовок и данные
            header = rows[0]      # Первая строка - заголовок
            data_rows = rows[1:]  # Остальные строки - данные
            
            # Записываем отладочную информацию
            self.logger.debug(f"Заголовок: {header}")
            self.logger.debug(f"Количество строк данных: {len(data_rows)}")
            
            # Определяем типы полей для сортировки
            field_types = {}
            for field_config in self.common_sort_fields:
                field_name = field_config['name']
                
                if field_config['type'] == 'auto':
                    # Автоматически определяем тип поля
                    column_index = header.index(field_name)  # Индекс колонки
                    # Берем первые 100 строк для определения типа
                    sample_values = [row[column_index] for row in data_rows[:100]]
                    field_types[field_name] = self.detect_field_type(sample_values)
                    self.logger.info(f"Автоопределение типа поля '{field_name}': {field_types[field_name]}")
                else:
                    # Используем заданный тип
                    field_types[field_name] = field_config['type']
                    
            # Создаем функцию для сортировки строк
            def sort_key(row):
                key_values = []
                # Проходим по всем полям для сортировки
                for field_config in self.common_sort_fields:
                    field_name = field_config['name']
                    column_index = header.index(field_name)  # Индекс колонки
                    
                    # Получаем значение из строки (или пустую строку, если колонки нет)
                    value = row[column_index] if column_index < len(row) else ''
                    
                    # Преобразуем значение для сортировки
                    sort_value = self.sort_value(value, field_types[field_name])
                    key_values.append(sort_value)
                    
                # Возвращаем кортеж для сортировки по нескольким полям
                return tuple(key_values)
                
            # Сортируем данные по заданному ключу
            reverse = sort_config['order'] == 'desc'  # Определяем направление сортировки
            sorted_data = sorted(data_rows, key=sort_key, reverse=reverse)
            
            # Записываем информацию о сортировке
            self.logger.info(f"Данные отсортированы. Порядок: {'убывание' if reverse else 'возрастание'}")
            
            # Записываем отсортированный файл, сохраняя исходное форматирование
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                # Записываем заголовок как есть
                f.write(sort_config['delimiter'].join(header) + '\n')
                # Записываем отсортированные данные как есть
                for row in sorted_data:
                    f.write(sort_config['delimiter'].join(row) + '\n')
                
            self.logger.info(f"Отсортированный файл сохранен: {output_file}")
            
        except Exception as e:
            # В случае ошибки записываем её в лог и пробрасываем дальше
            self.logger.error(f"Ошибка при сортировке файла {input_file}: {str(e)}")
            raise
            
    def process_files(self):
        """Основной метод обработки всех файлов"""
        
        self.logger.info("Начинаю обработку CSV файлов")
        
        # Проверяем, что есть поля для сортировки
        if not self.common_sort_fields:
            self.logger.error("Нет общих полей для сортировки. Обработка прервана.")
            return
            
        # Формируем пути к папкам
        base_path = Path(self.config['base_path'])
        input_dir = base_path / self.config['input_subfolder']
        output_dir = base_path / self.config['output_subfolder']
        
        # Проходим по всем файлам из конфигурации
        for filename in self.config['input_files']:
            # Формируем путь к входному файлу
            input_file = input_dir / f"{filename}.csv"
            
            # Проверяем существование входного файла
            if not input_file.exists():
                self.logger.error(f"Входной файл не найден: {input_file}")
                continue
                
            # Формируем имя выходного файла (добавляем _SORT)
            output_filename = f"{filename}_SORT.csv"
            output_file = output_dir / output_filename
            
            self.logger.info(f"Обрабатываю файл: {filename}")
            
            try:
                # Сортируем файл
                self.sort_csv_file(input_file, output_file, self.config['sort_config'])
                self.logger.info(f"Файл {filename} успешно обработан")
            except Exception as e:
                # В случае ошибки записываем её в лог
                self.logger.error(f"Не удалось обработать файл {filename}: {str(e)}")
                
        self.logger.info("Обработка завершена")


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ ПРОГРАММЫ
# =============================================================================

def main():
    """Главная функция программы"""
    
    # Конфигурация программы - все параметры заданы здесь
    config = {
        'base_path': BASE_PATH,                    # Базовая рабочая папка
        'input_subfolder': INPUT_SUBFOLDER,        # Подпапка для входных файлов
        'output_subfolder': OUTPUT_SUBFOLDER,      # Подпапка для выходных файлов
        'logs_subfolder': LOGS_SUBFOLDER,          # Подпапка для логов
        'log_filename': LOG_FILENAME,              # Имя файла логов (без расширения)
        'input_files': INPUT_FILES,                # Имена файлов без расширения .csv
        'sort_config': SORT_CONFIG                 # Конфигурация сортировки
    }
    
    # Создаем экземпляр сортировщика с заданной конфигурацией
    sorter = CSVSorter(config)
    
    # Запускаем процесс обработки файлов
    sorter.process_files()


# =============================================================================
# ТОЧКА ВХОДА В ПРОГРАММУ
# =============================================================================

if __name__ == "__main__":
    main()
