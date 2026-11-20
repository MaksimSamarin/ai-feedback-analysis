"""Потоковое чтение CSV без дополнительных библиотек."""
import codecs
import csv
from pathlib import Path
from app.config import MAX_UPLOAD_BYTES

CSV_SHEET = "Данные"
csv.field_size_limit(MAX_UPLOAD_BYTES)


def csv_format(path: Path):
    if path.stat().st_size == 0:
        raise ValueError("CSV-файл пуст")
    encoding = "utf-8-sig"
    decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(65536):
                if b"\x00" in chunk:
                    raise ValueError("CSV содержит двоичные данные")
                decoder.decode(chunk)
            decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        encoding = "cp1251"
    try:
        with path.open(encoding=encoding, newline="") as stream:
            sample = stream.read(65536)
    except UnicodeDecodeError as exc:
        raise ValueError("Не удалось прочитать CSV: ожидается UTF-8 или Windows-1251") from exc
    if not sample.strip():
        raise ValueError("CSV-файл пуст")
    if any(ord(c) < 32 and c not in "\r\n\t" for c in sample):
        raise ValueError("CSV содержит недопустимые управляющие символы")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        delimiter = dialect.delimiter
    except csv.Error:
        header = sample.splitlines()[0]
        delimiter = max(",;\t", key=header.count)
    return encoding, delimiter


class CsvWorkbook:
    sheet_names = [CSV_SHEET]

    def __init__(self, path: Path):
        self.path = path
        self.encoding, self.delimiter = csv_format(path)
        self.stream = None

    def get_sheet_by_name(self, name):
        if name != CSV_SHEET:
            raise ValueError(f"Лист не найден: {name}")
        return self

    def iter_rows(self):
        self.close()
        self.stream = self.path.open(encoding=self.encoding, newline="")
        source_line = 1
        try:
            reader = csv.reader(self.stream, delimiter=self.delimiter, strict=True)
            header = next(reader, None)
            if not header:
                raise ValueError("CSV не содержит заголовков")
            normalized = [str(v).strip() or f"Column {i+1}" for i, v in enumerate(header)]
            if len(set(normalized)) != len(normalized):
                raise ValueError("CSV содержит повторяющиеся названия колонок")
            yield normalized
            while True:
                source_line = reader.line_num + 1
                try:
                    row = next(reader)
                except StopIteration:
                    break
                if any(any(ord(c) < 32 and c not in "\r\n\t" for c in value) for value in row):
                    raise ValueError(f"CSV, строка {reader.line_num}: недопустимые управляющие символы")
                if len(row) > len(header):
                    raise ValueError(f"CSV, строка {reader.line_num}: полей больше, чем колонок заголовка")
                yield row
        except (csv.Error, UnicodeDecodeError) as exc:
            raise ValueError(f"CSV, строка {source_line}: повреждённая запись или незакрытая кавычка "
                             f"(чтение до строки {reader.line_num})") from exc
        finally:
            self.close()

    def close(self):
        if self.stream:
            self.stream.close()
            self.stream = None
