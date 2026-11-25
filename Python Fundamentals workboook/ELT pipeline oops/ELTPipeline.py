""" 
data_pipeline.py — OOP Lab Implementation 
""" 
from abc import ABC, abstractmethod 
from datetime import datetime, timezone
import csv, json, time, logging 
  
logging.basicConfig(level=logging.INFO, 
    format='%(asctime)s | %(levelname)s | %(message)s') 
logger = logging.getLogger(__name__) 
  
  
# ══════════════════════════════════════════════════════ 
# EXTRACTORS 
# ══════════════════════════════════════════════════════ 
class BaseExtractor(ABC): 
    """Abstract base for all extractors.""" 
    def __init__(self, source_name: str): 
        self.source_name     = source_name 
        self.rows_extracted  = 0 
        self._extract_time   = 0.0 
  
    @abstractmethod 
    def extract(self) -> list[dict]: ... 
  
    def __repr__(self): 
        return f'{self.__class__.__name__}({self.source_name!r})' 
  
  
class CSVExtractor(BaseExtractor): 
    def __init__(self, source_name: str, file_path: str): 
        super().__init__(source_name) 
        self.file_path = file_path 
  
    def extract(self) -> list[dict]: 
        start = time.time() 
        logger.info(f'Extracting from CSV: {self.file_path}') 
        with open(self.file_path, newline='') as f: 
            records = list(csv.DictReader(f)) 
        self.rows_extracted = len(records) 
        self._extract_time  = time.time() - start 
        logger.info(f'Extracted {self.rows_extracted} rows in {self._extract_time:.2f}s') 
        return records 
  
  
class JSONExtractor(BaseExtractor): 
    def __init__(self, source_name: str, file_path: str): 
        super().__init__(source_name) 
        self.file_path = file_path 
  
    def extract(self) -> list[dict]: 
        start = time.time() 
        logger.info(f'Extracting from JSON: {self.file_path}') 
        with open(self.file_path) as f: 
            data = json.load(f) 
        records = data if isinstance(data, list) else [data] 
        self.rows_extracted = len(records) 
        self._extract_time  = time.time() - start 
        return records 
  
  
# ══════════════════════════════════════════════════════ 
# TRANSFORMERS 
# ══════════════════════════════════════════════════════ 
class BaseTransformer(ABC): 
    @abstractmethod 
    def transform(self, data: list[dict]) -> list[dict]: ... 
  
  
class CleanTransformer(BaseTransformer): 
    def __init__(self, null_cols: list[str] = None,  drop_duplicates: bool = True): 
        self.null_cols       = null_cols or [] 
        self.drop_duplicates = drop_duplicates 
  
    def transform(self, data: list[dict]) -> list[dict]: 
        logger.info(f'Cleaning {len(data)} rows...') 
        # Drop rows missing required columns 
        if self.null_cols: 
            data = [r for r in data 
                    if all(r.get(c) for c in self.null_cols)] 
        # Remove duplicate rows 
        if self.drop_duplicates: 
            seen, unique = set(), [] 
            for r in data: 
                key = str(sorted(r.items())) 
                if key not in seen: 
                    seen.add(key); unique.append(r) 
            data = unique 
        logger.info(f'{len(data)} rows after cleaning.') 
        return data 

class UpperCaseTransformer(BaseTransformer):
    def transform(self,data):
        if not data:
            return data
        for row in data:
            for key,val in row.items():
                if isinstance(val,str):
                    row[key]=val.upper()
        return data;

class ChainedTransformer(BaseTransformer):
    def __init__(self,transformers:list):
        self.transformers=transformers

    def transform(self,data):
        for t in self.transformers:
            data=t.transform(data)
        return data
            
    
  
# ══════════════════════════════════════════════════════ 
# LOADERS 
# ══════════════════════════════════════════════════════ 
class BaseLoader(ABC): 
    def __init__(self, destination: str): 
        self.destination = destination 
        self.rows_loaded = 0 
  
    @abstractmethod 
    def load(self, data: list[dict]) -> int: ... 
  
  
class CSVLoader(BaseLoader): 
    def load(self, data: list[dict]) -> int: 
        if not data: return 0 
        logger.info(f'Writing {len(data)} rows to {self.destination}') 
        with open(self.destination, 'w', newline='') as f: 
            writer = csv.DictWriter(f, fieldnames=data[0].keys()) 
            writer.writeheader() 
            writer.writerows(data) 
        self.rows_loaded = len(data) 
        return self.rows_loaded 
  
  
class JSONLoader(BaseLoader): 
    def load(self, data: list[dict]) -> int: 
        logger.info(f'Writing {len(data)} rows to {self.destination}') 
        with open(self.destination, 'w') as f: 
            json.dump(data, f, indent=2) 
        self.rows_loaded = len(data) 
        return self.rows_loaded 
  
  
# ══════════════════════════════════════════════════════ 
# DATA PIPELINE ORCHESTRATOR 
# ══════════════════════════════════════════════════════ 
class DataPipeline: 
    """ 
    Orchestrates Extract → Transform → Load. 
    Accepts any BaseExtractor, BaseTransformer, BaseLoader. 
    """ 
  
    def __init__( 
        self, 
        name:        str, 
        extractor:   BaseExtractor, 
        transformer: BaseTransformer, 
        loader:      BaseLoader, 
        max_retries: int = 3, 
    ): 
        self.name        = name 
        self.extractor   = extractor 
        self.transformer = transformer 
        self.loader      = loader 
        self.max_retries = max_retries 
        self.__metrics   = {          # encapsulated — private 
            'status':        'idle', 
            'rows_extracted': 0, 
            'rows_transformed': 0, 
            'rows_loaded':    0, 
            'duration_sec':   0.0, 
            'started_at':     None, 
            'finished_at':    None, 
            'errors':         [], 
        } 
  
    # ── Public interface ────────────────────────────── 
    def run(self) -> dict: 
        """Execute the full ETL pipeline. Returns metrics.""" 
        self.__metrics['started_at'] = datetime.now(timezone.utc).isoformat()
        self.__metrics['status']     = 'running' 
        start = time.time() 
        logger.info(f'Pipeline [{self.name}] STARTED') 
  
        try: 
            raw_data    = self._extract() 
            clean_data  = self._transform(raw_data) 
            self._load(clean_data) 
            self.__metrics['status'] = 'success' 
            logger.info(f'Pipeline [{self.name}] COMPLETED successfully.') 
  
        except Exception as e: 
            self.__metrics['status'] = 'failed' 
            self.__metrics['errors'].append(str(e)) 
            logger.error(f'Pipeline [{self.name}] FAILED: {e}') 
            # raise 
  
        finally: 
            self.__metrics['duration_sec']  = round(time.time() - start, 10) 
            self.__metrics['finished_at']   = datetime.now(timezone.utc).isoformat() 
  
        return self.get_metrics() 
  
    @property 
    def status(self) -> str: 
        return self.__metrics['status'] 
  
    def get_metrics(self) -> dict: 
        """Return a copy of pipeline metrics (read-only).""" 
        return dict(self.__metrics) 
  
    # ── Private ETL steps ───────────────────────────── 
    def _extract(self) -> list[dict]: 

        for i in range(self.max_retries +1):
            try:
                logger.info('  [E] Extract phase...') 
                data = self.extractor.extract() 
            except Exception as e:
                self.__metrics['errors'].append(str(e))
                if i == self.max_retries:
                  raise
                time.sleep(2 ** i)
            else:
                self.__metrics['rows_extracted'] = len(data)
                return data 
  
    def _transform(self, data: list[dict]) -> list[dict]: 
        for i in range(self.max_retries +1):
                    try:
                        logger.info('  [T] Transform phase...') 
                        result = self.transformer.transform(data) 
                    except Exception as e:
                        self.__metrics['errors'].append(str(e))
                        if i == self.max_retries:
                          raise
                        time.sleep(2 ** i)
                    else:
                        self.__metrics['rows_transformed'] = len(result)
                        return result 
  
    def _load(self, data: list[dict]) -> None:
        for i in range(self.max_retries +1):
            try:
                logger.info('  [L] Load phase...') 
                n = self.loader.load(data)  
            except Exception as e:
                self.__metrics['errors'].append(str(e))
                if i == self.max_retries:
                 raise
                time.sleep(2 ** i)
            else:
                self.__metrics['rows_loaded'] = n
                return n 

        
  
    def __repr__(self): 
        return ( 
            f'DataPipeline({self.name!r}, ' 
            f'extractor={self.extractor!r}, ' 
            f'loader={self.loader.destination!r})' 
        ) 
  
  
# ══════════════════════════════════════════════════════ 
# USAGE EXAMPLE 
# ══════════════════════════════════════════════════════ 
if __name__ == '__main__': 
    pipeline = DataPipeline( 
        name        = 'customer_etl', 
        extractor   = CSVExtractor('crm', 'customers_raw.csv'), 
        transformer =ChainedTransformer( [CleanTransformer( 
                          null_cols=['email', 'name'], 
                          drop_duplicates=True),
                          UpperCaseTransformer()]), 
        loader      = CSVLoader('customers_clean.csv'), 
    ) 
  
    metrics = pipeline.run() 
    print('\n--- Pipeline Metrics ---') 
    for k, v in metrics.items(): 
        print(f'  {k:<20}: {v}') 