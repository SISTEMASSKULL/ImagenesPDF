"""
Sistema de logging estructurado para ImagenesPDF.

Proporciona logging unificado con:
- Logs estructurados (JSON) y legibles por humanos
- Rotación automática de archivos
- Niveles configurables por módulo
- Contexto de procesamiento (PDF, página, item)
- Métricas de rendimiento integradas
- Formato colorizado para consola

Uso:
    from imagenespdf.logging_setup import get_logger, setup_logging
    
    # Configurar sistema
    setup_logging(level="INFO", log_dir="out/logs")
    
    # En cada módulo
    logger = get_logger(__name__)
    logger.info("Procesando PDF", pdf_file="catalog.pdf", pages=100)
"""

import os
import sys
import json
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union, TextIO
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import time
import traceback

try:
    import structlog
    HAS_STRUCTLOG = True
except ImportError:
    HAS_STRUCTLOG = False

try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.text import Text
    HAS_RICH = True
except ImportError:
    HAS_RICH = False


@dataclass
class ProcessingContext:
    """Contexto de procesamiento actual."""
    pdf_file: Optional[str] = None
    pdf_id: Optional[int] = None
    supplier: Optional[str] = None
    page_num: Optional[int] = None
    item_id: Optional[int] = None
    operation: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario para logging."""
        return {k: v for k, v in asdict(self).items() if v is not None}


class ColorizedFormatter(logging.Formatter):
    """Formatter con colores para consola."""
    
    # Códigos de color ANSI
    COLORS = {
        'DEBUG': '\033[36m',     # Cian
        'INFO': '\033[32m',      # Verde
        'WARNING': '\033[33m',   # Amarillo
        'ERROR': '\033[31m',     # Rojo
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'       # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """Formatear record con colores."""
        if not hasattr(record, 'no_color') or not record.no_color:
            color = self.COLORS.get(record.levelname, '')
            reset = self.COLORS['RESET']
            record.levelname = f"{color}{record.levelname}{reset}"
            
        return super().format(record)


class StructuredFormatter(logging.Formatter):
    """Formatter para logs estructurados en JSON."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Formatear record como JSON estructurado."""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Agregar contexto si existe
        if hasattr(record, 'context') and record.context:
            log_data.update(record.context)
            
        # Agregar campos extras
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'lineno', 'funcName', 'created', 
                          'msecs', 'relativeCreated', 'thread', 'threadName', 
                          'processName', 'process', 'context', 'no_color']:
                log_data[key] = value
                
        # Agregar exception info si existe
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info)
            }
            
        return json.dumps(log_data, ensure_ascii=False, default=str)


class ContextLogger:
    """
    Logger con contexto de procesamiento.
    
    Mantiene contexto automático y permite logging estructurado.
    """
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.context = ProcessingContext()
        self._start_times: Dict[str, float] = {}
        
    def set_context(self, **kwargs) -> None:
        """Actualizar contexto de procesamiento."""
        for key, value in kwargs.items():
            if hasattr(self.context, key):
                setattr(self.context, key, value)
                
    def clear_context(self) -> None:
        """Limpiar contexto de procesamiento."""
        self.context = ProcessingContext()
        
    def _log_with_context(self, level: int, msg: str, **kwargs) -> None:
        """Log con contexto automático."""
        context = self.context.to_dict()
        context.update(kwargs)
        
        # Crear record personalizado
        extra = {'context': context}
        self.logger.log(level, msg, extra=extra)
        
    def debug(self, msg: str, **kwargs) -> None:
        """Log nivel DEBUG con contexto."""
        self._log_with_context(logging.DEBUG, msg, **kwargs)
        
    def info(self, msg: str, **kwargs) -> None:
        """Log nivel INFO con contexto."""
        self._log_with_context(logging.INFO, msg, **kwargs)
        
    def warning(self, msg: str, **kwargs) -> None:
        """Log nivel WARNING con contexto."""
        self._log_with_context(logging.WARNING, msg, **kwargs)
        
    def error(self, msg: str, **kwargs) -> None:
        """Log nivel ERROR con contexto."""
        self._log_with_context(logging.ERROR, msg, **kwargs)
        
    def critical(self, msg: str, **kwargs) -> None:
        """Log nivel CRITICAL con contexto."""
        self._log_with_context(logging.CRITICAL, msg, **kwargs)
        
    def exception(self, msg: str, **kwargs) -> None:
        """Log excepción con contexto."""
        context = self.context.to_dict()
        context.update(kwargs)
        extra = {'context': context}
        self.logger.exception(msg, extra=extra)
        
    def start_timer(self, operation: str) -> None:
        """Iniciar timer para operación."""
        self._start_times[operation] = time.time()
        self.debug(f"Iniciando {operation}")
        
    def end_timer(self, operation: str) -> float:
        """Finalizar timer y log duración."""
        if operation not in self._start_times:
            self.warning(f"Timer no encontrado para operación: {operation}")
            return 0.0
            
        duration = time.time() - self._start_times[operation]
        del self._start_times[operation]
        
        self.info(f"Completado {operation}", 
                 duration_seconds=round(duration, 3))
        return duration
        
    @contextmanager
    def timer(self, operation: str):
        """Context manager para timing automático."""
        self.start_timer(operation)
        try:
            yield
        finally:
            self.end_timer(operation)
            
    @contextmanager
    def processing_context(self, **kwargs):
        """Context manager para contexto temporal."""
        # Guardar contexto actual
        old_context = ProcessingContext(**asdict(self.context))
        
        # Aplicar nuevo contexto
        self.set_context(**kwargs)
        
        try:
            yield
        finally:
            # Restaurar contexto
            self.context = old_context


class LoggingManager:
    """Gestor principal del sistema de logging."""
    
    def __init__(self):
        self.is_configured = False
        self.log_dir: Optional[Path] = None
        self.loggers: Dict[str, ContextLogger] = {}
        
    def setup(self, 
              level: Union[str, int] = "INFO",
              log_dir: Optional[Union[str, Path]] = None,
              console_output: bool = True,
              file_output: bool = True,
              json_output: bool = True,
              max_file_size: int = 50 * 1024 * 1024,  # 50MB
              backup_count: int = 5) -> None:
        """
        Configurar sistema de logging.
        
        Args:
            level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: Directorio para archivos de log
            console_output: Si mostrar logs en consola
            file_output: Si guardar logs en archivos
            json_output: Si generar logs estructurados JSON
            max_file_size: Tamaño máximo por archivo de log
            backup_count: Cantidad de archivos de backup a mantener
        """
        if isinstance(level, str):
            level = getattr(logging, level.upper())
            
        # Preparar directorio de logs
        if log_dir:
            self.log_dir = Path(log_dir)
            self.log_dir.mkdir(parents=True, exist_ok=True)
        elif file_output or json_output:
            self.log_dir = Path("out/logs")
            self.log_dir.mkdir(parents=True, exist_ok=True)
            
        # Configurar root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        
        # Limpiar handlers existentes
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            
        # Handler de consola
        if console_output:
            if HAS_RICH:
                console_handler = RichHandler(
                    console=Console(),
                    show_time=True,
                    show_path=True,
                    markup=True
                )
            else:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setFormatter(ColorizedFormatter(
                    '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                    datefmt='%H:%M:%S'
                ))
            
            console_handler.setLevel(level)
            root_logger.addHandler(console_handler)
            
        # Handler de archivo principal
        if file_output and self.log_dir:
            file_handler = logging.handlers.RotatingFileHandler(
                self.log_dir / "imagenespdf.log",
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            ))
            file_handler.setLevel(level)
            root_logger.addHandler(file_handler)
            
        # Handler JSON estructurado
        if json_output and self.log_dir:
            json_handler = logging.handlers.RotatingFileHandler(
                self.log_dir / "imagenespdf.json",
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding='utf-8'
            )
            json_handler.setFormatter(StructuredFormatter())
            json_handler.setLevel(level)
            root_logger.addHandler(json_handler)
            
        # Handler de errores separado
        if file_output and self.log_dir:
            error_handler = logging.handlers.RotatingFileHandler(
                self.log_dir / "errors.log",
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding='utf-8'
            )
            error_handler.setFormatter(logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s\n%(pathname)s:%(lineno)d'
            ))
            error_handler.setLevel(logging.ERROR)
            root_logger.addHandler(error_handler)
            
        self.is_configured = True
        
        # Log inicial
        logger = self.get_logger("imagenespdf.logging")
        logger.info("Sistema de logging configurado", 
                   level=logging.getLevelName(level),
                   log_dir=str(self.log_dir) if self.log_dir else None,
                   console_output=console_output,
                   file_output=file_output,
                   json_output=json_output)
    
    def get_logger(self, name: str) -> ContextLogger:
        """Obtener logger con contexto para un módulo."""
        if name not in self.loggers:
            base_logger = logging.getLogger(name)
            self.loggers[name] = ContextLogger(base_logger)
        return self.loggers[name]
        
    def shutdown(self) -> None:
        """Cerrar sistema de logging."""
        logging.shutdown()
        self.is_configured = False
        self.loggers.clear()


# Instancia global
_logging_manager = LoggingManager()


def setup_logging(**kwargs) -> None:
    """Configurar sistema de logging (función de conveniencia)."""
    _logging_manager.setup(**kwargs)


def get_logger(name: str) -> ContextLogger:
    """Obtener logger con contexto (función de conveniencia)."""
    if not _logging_manager.is_configured:
        # Auto-configurar con valores por defecto
        _logging_manager.setup()
    return _logging_manager.get_logger(name)


def shutdown_logging() -> None:
    """Cerrar sistema de logging (función de conveniencia)."""
    _logging_manager.shutdown()


def log_system_info() -> None:
    """Log información del sistema al inicio."""
    logger = get_logger("imagenespdf.system")
    
    logger.info("Iniciando ImagenesPDF", 
               python_version=sys.version.split()[0],
               platform=sys.platform,
               working_dir=os.getcwd(),
               has_structlog=HAS_STRUCTLOG,
               has_rich=HAS_RICH)


if __name__ == "__main__":
    # Modo de prueba
    setup_logging(level="DEBUG")
    
    logger = get_logger("test")
    
    # Probar diferentes niveles
    logger.debug("Mensaje de debug")
    logger.info("Información general", item_count=42)
    logger.warning("Advertencia", issue="archivo_no_encontrado")
    logger.error("Error procesando", error_code="PDF_CORRUPT")
    
    # Probar contexto
    logger.set_context(pdf_file="test.pdf", supplier="DEPO")
    logger.info("Con contexto")
    
    # Probar timer
    with logger.timer("operacion_test"):
        time.sleep(0.1)
        
    # Probar contexto temporal
    with logger.processing_context(page_num=5, item_id=123):
        logger.info("Procesando ítem")
        
    logger.clear_context()
    logger.info("Sin contexto")
    
    print("Prueba completada. Revisa los archivos de log en out/logs/")