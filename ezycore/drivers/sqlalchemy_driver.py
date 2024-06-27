from __future__ import annotations
from typing import Any, Dict, Iterator, Optional, Tuple, Union
from sqlalchemy import create_engine, MetaData, select, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel

from .core import Driver, RESULT, StrOrBytesPath

Base = declarative_base()

class SQLAlchemyDriver(Driver):
    """Default implementation for the SQLAlchemy driver
    
    Parameters
    ----------
    models: Dict[:class:`str`, :class:`BaseModel`]
        Models to convert fetched results to, mapping must be table name to model
    model_maps: Dict[:class:`str`, :class:`str`]
        Mapping from model key to database table name
    """
    
    def __init__(self,
                 database_url: str,
                 models: Dict[str, BaseModel] = dict(),
                 model_maps: Dict[str, str] = dict()
                ) -> None:
        self.__engine = create_engine(database_url)
        self.__Session = sessionmaker(bind=self.__engine)
        self.__session = self.__Session()
        
        self.__models: Dict[str, BaseModel] = models
        self.__headers: Dict[str, Tuple[str]] = dict()
        self.__maps: Dict[str, str] = model_maps
        self.__rev_map: Dict[str, str] = {v: k for k, v in self.__maps.items()}
        
        self._read_heads()

    def _read_heads(self) -> None:
        metadata = MetaData(bind=self.__engine)
        metadata.reflect()
        for table in metadata.tables.values():
            self.__headers[table.name] = tuple(c.name for c in table.columns)
    
    def _result_to_output(self, head: str, model: Optional[BaseModel], results) -> Iterator[dict]:
        for result in results:
            data = {self.__headers[head][i]: v for i, v in enumerate(result)}
            if not model:
                yield data
            else:
                yield model(**data).dict()
    
    def _get_model(self, location: str) -> Optional[BaseModel]:
        return self.__models.get(
            location, 
            self.__models.get(self.__maps.get(location), self.__models.get(self.__rev_map.get(location)))
        )
    
    def _model_fits(self, location: str) -> bool:
        model = self._get_model(location)
        if not model:
            return False
        keys = model.__fields__.keys()
        return set(self.__headers[location]).issubset(set(keys))
    
    def map_to_model(self, **kwds) -> None:
        self.__maps.update(kwds)
        self.__rev_map = {v: k for k, v in self.__maps.items()}
    
    def fetch(self, location: str, condition: str = '', limit_result: int = -1, 
              model: BaseModel = None, *, raw: str = None, no_handle: bool = False, ignore_model: bool = False,
              parameters: Tuple[Any] = tuple()
    ) -> Optional[Iterator[RESULT]]:
        if model and not self._get_model(location):
            self.__models[location] = model
        model = self._get_model(location)
        table = self.__maps.get(location, location)
        
        try:
            query = select([table])
            if condition:
                query = query.where(text(condition))
            if limit_result > 0:
                query = query.limit(limit_result)
            results = self.__session.execute(query).fetchall()
        except SQLAlchemyError as e:
            print(f"Error fetching data: {e}")
            return None
        
        if not results: return None
        if no_handle: return iter(results)
        return self._result_to_output(table, model if not ignore_model else None, results)
    
    def fetch_one(self, location: str, condition: Any = None, model: BaseModel = None, 
                  *, raw: Any = None, no_handle: bool = False, ignore_model: bool = False,
                  parameters: Tuple[Any] = tuple()
    ) -> Optional[RESULT]:
        if model and not self._get_model(location):
            self.__models[location] = model
        model = self._get_model(location)
        table = self.__maps.get(location, location)
        
        try:
            query = select([table])
            if condition:
                query = query.where(text(condition))
            query = query.limit(1)
            result = self.__session.execute(query).fetchone()
        except SQLAlchemyError as e:
            print(f"Error fetching data: {e}")
            return None
        
        if not result: return None
        if no_handle: return result
        return next(self._result_to_output(table, model if not ignore_model else None, [result]))
    
    def export(self, location: str, stream: Iterator[Union[dict, BaseModel]], include: set = None, exclude: set = None) -> None:
        assert self._model_fits(location), "Incorrect model or no model binded for this table"
        model = self._get_model(location)
        include = include or set(self.__headers[location])
        exclude = exclude or set()

        for data in stream:
            if isinstance(data, dict):
                if model:
                    data = model(**data)
            data = data.dict(include=include, exclude=exclude)
            
            table = self.__maps.get(location, location)
            instance = self.__session.get_bind().table(table)(**data)
            
            try:
                self.__session.add(instance)
                self.__session.commit()
            except SQLAlchemyError as e:
                print(f"Error exporting data: {e}")
                self.__session.rollback()
