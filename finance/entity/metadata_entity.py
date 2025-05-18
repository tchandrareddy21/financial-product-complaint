import os
import sys
from collections import namedtuple
from finance.logger import logger
from finance.exception import FinancialException
from finance.utils import read_yaml_file, write_yaml_file


DataIngestionMetadataInfo = namedtuple("DataIngestionMetadataInfo", ["from_date", "to_date", "data_file_path"])

class DataIngestionMetadata:
    def __init__(self, metadata_file_path):
        self.metedata_file_path = metadata_file_path
        
    @property
    def is_metadata_file_present(self):
        return  os.path.exists(self.metedata_file_path)
    
    def write_metadata_info(self, from_date: str, to_date: str, data_file_path: str):
        try:
            metadatainfo = DataIngestionMetadataInfo(
                from_date=from_date,
                to_date=to_date,
                data_file_path=data_file_path
            )
            write_yaml_file(file_path=self.metedata_file_path, data=metadatainfo._asdict())
        except Exception as e:
            raise FinancialException(e, sys)
        
    def get_metadata_info(self) -> DataIngestionMetadataInfo:
        try:
            if not self.is_metadata_file_present:
                raise Exception("No metadata file available")
            metadata = read_yaml_file(file_path= self.metedata_file_path)
            metadata_info = DataIngestionMetadataInfo(**(metadata))
            logger.info(f"Metadata info: {metadata_info}")
            return metadata_info
        except Exception as e:
            raise FinancialException(e, sys)

