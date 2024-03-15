from enum import Enum
from csv import DictReader
from datetime import datetime
from marshmallow import Schema
from schema.accelerometer_schema import AccelerometerSchema
from schema.gps_schema import GpsSchema
from schema.parking_schema import ParkingEmptyCount
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
import config


class FileDatasource:
    class DataKeys(Enum):
        ACCELEROMETER = 0
        GPS = 1
        PARKING = 2

    def __init__(
            self,
            accelerometer_filename: str,
            gps_filename: str,
            parking_filename: str
    ) -> None:
        self.readers = [None] * len(FileDatasource.DataKeys)
        self.readers[FileDatasource.DataKeys.ACCELEROMETER.value] = DatasrcReader(accelerometer_filename,
                                                                                  AccelerometerSchema())
        self.readers[FileDatasource.DataKeys.GPS.value] = DatasrcReader(gps_filename, GpsSchema())
        self.readers[FileDatasource.DataKeys.PARKING.value] = DatasrcReader(parking_filename, ParkingEmptyCount())

    def read(self, batch_size) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        for reader in self.readers:
            if not reader.reader:
                raise Exception("CSV Readers are not initialized. Call startReading first.")

        result = [None] * batch_size

        try:
            for i in range(batch_size):
                accelerometer = Accelerometer(**self.readers[FileDatasource.DataKeys.ACCELEROMETER.value].read())
                gps = Gps(**self.readers[FileDatasource.DataKeys.GPS.value].read())
                parking_count = self.readers[FileDatasource.DataKeys.PARKING.value].read()["empty_count"]
                result[i] = AggregatedData(accelerometer, gps, datetime.now(), config.USER_ID), Parking(parking_count,
                                                                                                        gps)
            return result
        except Exception as err:
            print(f"Validation err: {err}")
            return []

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        for reader in self.readers:
            reader.startReading()

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        for reader in self.readers:
            reader.stopReading()


class DatasrcReader:
    """Helper class for reading files.
     This class will read the file and load an object based on the schema"""
    filename: str
    reader: DictReader

    def __init__(self, filename, schema: Schema):
        self.filename = filename
        self.schema = schema

    def startReading(self):
        self.file = open(self.filename, 'r')
        self.reader = DictReader(self.file)

    def read(self):
        row = next(self.reader, None)
        if row is None:
            self.reset()
            row = next(self.reader, None)
        return self.schema.load(row)

    def reset(self):
        self.file.seek(0)
        self.reader = DictReader(self.file)

    def finishReading(self):
        if self.file:
            self.file.close()