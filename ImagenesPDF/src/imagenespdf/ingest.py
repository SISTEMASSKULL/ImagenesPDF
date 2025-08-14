"""
Sistema de ingesta de PDFs para ImagenesPDF.

Maneja la lectura, validación y extracción de metadatos de catálogos PDF:
- Detección automática de formato y estructura
- Extracción de metadatos (título, autor, páginas, etc.)
- Cálculo de hashes para detección de cambios
- Validación de integridad y formato
- Soporte para múltiples librerías PDF (PyMuPDF, pdfplumber, pypdfium2)
- Cache de metadatos para optimización

Integra con el sistema de archivos y logging para trazabilidad completa.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Union, Iterator
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
import json
import tempfile

# Librerías PDF en orden de preferencia
try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    import pypdfium2 as pdfium
    HAS_PYPDFIUM2 = True
except ImportError:
    HAS_PYPDFIUM2 = False

from .logging_setup import get_logger
from .utils_fs import calculate_file_hash, FileInfo
from .config import get_config_manager

logger = get_logger(__name__)


class PDFLibrary(Enum):
    """Librerías PDF disponibles."""
    PYMUPDF = "pymupdf"
    PDFPLUMBER = "pdfplumber" 
    PYPDFIUM2 = "pypdfium2"


class PDFStatus(Enum):
    """Estados posibles de un PDF."""
    VALID = "valid"
    CORRUPTED = "corrupted"
    ENCRYPTED = "encrypted"
    UNSUPPORTED = "unsupported"
    NOT_FOUND = "not_found"
    PERMISSION_DENIED = "permission_denied"


@dataclass
class PDFMetadata:
    """Metadatos extraídos de un PDF."""
    # Información básica del archivo
    file_path: Path
    file_size: int
    file_hash: str
    modified_time: datetime
    
    # Metadatos del PDF
    title: Optional[str] = None
    author: Optional[str] = None
    subject: Optional[str] = None
    creator: Optional[str] = None
    producer: Optional[str] = None
    creation_date: Optional[datetime] = None
    modification_date: Optional[datetime] = None
    
    # Información técnica
    page_count: int = 0
    pdf_version: Optional[str] = None
    is_encrypted: bool = False
    is_linearized: bool = False
    has_forms: bool = False
    has_annotations: bool = False
    
    # Información de contenido
    has_images: bool = False
    has_text: bool = False
    estimated_text_pages: int = 0
    estimated_image_pages: int = 0
    
    # Procesamiento
    library_used: Optional[PDFLibrary] = None
    status: PDFStatus = PDFStatus.VALID
    error_message: Optional[str] = None
    ingested_at: datetime = None
    
    def __post_init__(self):
        """Inicializar campos calculados."""
        if self.ingested_at is None:
            self.ingested_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario serializable."""
        data = asdict(self)
        # Convertir Path a string
        data['file_path'] = str(self.file_path)
        # Convertir datetime a ISO string
        for field in ['modified_time', 'creation_date', 'modification_date', 'ingested_at']:
            if data[field] is not None:
                data[field] = data[field].isoformat()
        # Convertir enums a valores
        if data['library_used'] is not None:
            data['library_used'] = data['library_used'].value
        data['status'] = data['status'].value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PDFMetadata':
        """Crear instancia desde diccionario."""
        # Convertir strings de vuelta a objetos
        data['file_path'] = Path(data['file_path'])
        for field in ['modified_time', 'creation_date', 'modification_date', 'ingested_at']:
            if data[field] is not None:
                data[field] = datetime.fromisoformat(data[field])
        if data['library_used'] is not None:
            data['library_used'] = PDFLibrary(data['library_used'])
        data['status'] = PDFStatus(data['status'])
        return cls(**data)


class PDFReader:
    """
    Lector abstracto de PDFs que selecciona la mejor librería disponible.
    """
    
    def __init__(self, preferred_library: Optional[PDFLibrary] = None):
        """
        Inicializar lector PDF.
        
        Args:
            preferred_library: Librería preferida (None para auto-selección)
        """
        self.preferred_library = preferred_library
        self.available_libraries = self._detect_available_libraries()
        logger.debug(f"Librerías PDF disponibles: {[lib.value for lib in self.available_libraries]}")
        
    def _detect_available_libraries(self) -> List[PDFLibrary]:
        """Detectar librerías PDF disponibles."""
        available = []
        if HAS_PYMUPDF:
            available.append(PDFLibrary.PYMUPDF)
        if HAS_PDFPLUMBER:
            available.append(PDFLibrary.PDFPLUMBER)
        if HAS_PYPDFIUM2:
            available.append(PDFLibrary.PYPDFIUM2)
        return available
        
    def _select_library(self, file_path: Path) -> Optional[PDFLibrary]:
        """
        Seleccionar mejor librería para un PDF específico.
        
        Args:
            file_path: Ruta del archivo PDF
            
        Returns:
            Librería seleccionada o None si no hay disponibles
        """
        if not self.available_libraries:
            return None
            
        # Usar librería preferida si está disponible
        if self.preferred_library and self.preferred_library in self.available_libraries:
            return self.preferred_library
            
        # Auto-selección basada en disponibilidad y capacidades
        # PyMuPDF es generalmente la más robusta
        if PDFLibrary.PYMUPDF in self.available_libraries:
            return PDFLibrary.PYMUPDF
        elif PDFLibrary.PDFPLUMBER in self.available_libraries:
            return PDFLibrary.PDFPLUMBER
        elif PDFLibrary.PYPDFIUM2 in self.available_libraries:
            return PDFLibrary.PYPDFIUM2
        
        return self.available_libraries[0] if self.available_libraries else None
        
    def _extract_with_pymupdf(self, file_path: Path) -> PDFMetadata:
        """Extraer metadatos usando PyMuPDF."""
        try:
            doc = fitz.open(str(file_path))
            
            metadata = PDFMetadata(
                file_path=file_path,
                file_size=file_path.stat().st_size,
                file_hash=calculate_file_hash(file_path),
                modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc),
                library_used=PDFLibrary.PYMUPDF
            )
            
            # Metadatos básicos
            pdf_metadata = doc.metadata
            metadata.title = pdf_metadata.get('title')
            metadata.author = pdf_metadata.get('author')
            metadata.subject = pdf_metadata.get('subject')
            metadata.creator = pdf_metadata.get('creator')
            metadata.producer = pdf_metadata.get('producer')
            
            # Fechas
            if pdf_metadata.get('creationDate'):
                try:
                    # PyMuPDF devuelve fechas en formato específico
                    creation_str = pdf_metadata['creationDate']
                    if creation_str.startswith('D:'):
                        creation_str = creation_str[2:16]  # D:YYYYMMDDHHmmSS
                        metadata.creation_date = datetime.strptime(creation_str, '%Y%m%d%H%M%S')
                except:
                    pass
                    
            if pdf_metadata.get('modDate'):
                try:
                    mod_str = pdf_metadata['modDate']
                    if mod_str.startswith('D:'):
                        mod_str = mod_str[2:16]
                        metadata.modification_date = datetime.strptime(mod_str, '%Y%m%d%H%M%S')
                except:
                    pass
            
            # Información técnica
            metadata.page_count = doc.page_count
            metadata.pdf_version = f"1.{doc.pdf_version()}"
            metadata.is_encrypted = doc.needs_pass
            metadata.is_linearized = doc.is_pdf
            metadata.has_forms = len(doc.get_widgets()) > 0
            
            # Análisis de contenido por página
            text_pages = 0
            image_pages = 0
            has_any_text = False
            has_any_images = False
            
            for page_num in range(min(doc.page_count, 10)):  # Muestrea primeras 10 páginas
                page = doc[page_num]
                
                # Verificar texto
                text = page.get_text().strip()
                if text:
                    text_pages += 1
                    has_any_text = True
                    
                # Verificar imágenes
                image_list = page.get_images()
                if image_list:
                    image_pages += 1
                    has_any_images = True
                    
                # Verificar anotaciones
                if page.get_annotations():
                    metadata.has_annotations = True
            
            # Extrapolar a todo el documento
            if doc.page_count > 10:
                ratio_text = text_pages / 10
                ratio_images = image_pages / 10
                metadata.estimated_text_pages = int(ratio_text * doc.page_count)
                metadata.estimated_image_pages = int(ratio_images * doc.page_count)
            else:
                metadata.estimated_text_pages = text_pages
                metadata.estimated_image_pages = image_pages
                
            metadata.has_text = has_any_text
            metadata.has_images = has_any_images
            
            doc.close()
            return metadata
            
        except Exception as e:
            logger.error(f"Error extrayendo metadatos con PyMuPDF de {file_path}", error=str(e))
            return PDFMetadata(
                file_path=file_path,
                file_size=file_path.stat().st_size if file_path.exists() else 0,
                file_hash=calculate_file_hash(file_path) if file_path.exists() else "",
                modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc) if file_path.exists() else datetime.now(timezone.utc),
                library_used=PDFLibrary.PYMUPDF,
                status=PDFStatus.CORRUPTED,
                error_message=str(e)
            )
            
    def _extract_with_pdfplumber(self, file_path: Path) -> PDFMetadata:
        """Extraer metadatos usando pdfplumber."""
        try:
            with pdfplumber.open(file_path) as pdf:
                metadata = PDFMetadata(
                    file_path=file_path,
                    file_size=file_path.stat().st_size,
                    file_hash=calculate_file_hash(file_path),
                    modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc),
                    library_used=PDFLibrary.PDFPLUMBER
                )
                
                # Metadatos básicos
                if hasattr(pdf, 'metadata') and pdf.metadata:
                    metadata.title = pdf.metadata.get('/Title')
                    metadata.author = pdf.metadata.get('/Author')
                    metadata.subject = pdf.metadata.get('/Subject')
                    metadata.creator = pdf.metadata.get('/Creator')
                    metadata.producer = pdf.metadata.get('/Producer')
                
                # Información técnica
                metadata.page_count = len(pdf.pages)
                
                # Análisis de contenido
                text_pages = 0
                image_pages = 0
                has_any_text = False
                has_any_images = False
                
                for i, page in enumerate(pdf.pages[:10]):  # Muestrea primeras 10 páginas
                    # Verificar texto
                    text = page.extract_text()
                    if text and text.strip():
                        text_pages += 1
                        has_any_text = True
                    
                    # Verificar imágenes (pdfplumber tiene acceso limitado a imágenes)
                    if hasattr(page, 'images') and page.images:
                        image_pages += 1
                        has_any_images = True
                
                # Extrapolar
                if len(pdf.pages) > 10:
                    ratio_text = text_pages / 10
                    ratio_images = image_pages / 10
                    metadata.estimated_text_pages = int(ratio_text * len(pdf.pages))
                    metadata.estimated_image_pages = int(ratio_images * len(pdf.pages))
                else:
                    metadata.estimated_text_pages = text_pages
                    metadata.estimated_image_pages = image_pages
                    
                metadata.has_text = has_any_text
                metadata.has_images = has_any_images
                
                return metadata
                
        except Exception as e:
            logger.error(f"Error extrayendo metadatos con pdfplumber de {file_path}", error=str(e))
            return PDFMetadata(
                file_path=file_path,
                file_size=file_path.stat().st_size if file_path.exists() else 0,
                file_hash=calculate_file_hash(file_path) if file_path.exists() else "",
                modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc) if file_path.exists() else datetime.now(timezone.utc),
                library_used=PDFLibrary.PDFPLUMBER,
                status=PDFStatus.CORRUPTED,
                error_message=str(e)
            )
            
    def _extract_with_pypdfium2(self, file_path: Path) -> PDFMetadata:
        """Extraer metadatos usando pypdfium2."""
        try:
            pdf = pdfium.PdfDocument(str(file_path))
            
            metadata = PDFMetadata(
                file_path=file_path,
                file_size=file_path.stat().st_size,
                file_hash=calculate_file_hash(file_path),
                modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc),
                library_used=PDFLibrary.PYPDFIUM2
            )
            
            # Información técnica
            metadata.page_count = len(pdf)
            
            # pypdfium2 tiene acceso limitado a metadatos
            # Análisis básico de contenido
            text_pages = 0
            has_any_text = False
            
            for i in range(min(len(pdf), 10)):  # Muestrea primeras 10 páginas
                page = pdf.get_page(i)
                textpage = page.get_textpage()
                text = textpage.get_text_range()
                
                if text and text.strip():
                    text_pages += 1
                    has_any_text = True
                    
                textpage.close()
                page.close()
            
            # Extrapolar
            if len(pdf) > 10:
                ratio_text = text_pages / 10
                metadata.estimated_text_pages = int(ratio_text * len(pdf))
            else:
                metadata.estimated_text_pages = text_pages
                
            metadata.has_text = has_any_text
            pdf.close()
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extrayendo metadatos con pypdfium2 de {file_path}", error=str(e))
            return PDFMetadata(
                file_path=file_path,
                file_size=file_path.stat().st_size if file_path.exists() else 0,
                file_hash=calculate_file_hash(file_path) if file_path.exists() else "",
                modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc) if file_path.exists() else datetime.now(timezone.utc),
                library_used=PDFLibrary.PYPDFIUM2,
                status=PDFStatus.CORRUPTED,
                error_message=str(e)
            )
    
    def extract_metadata(self, file_path: Union[str, Path]) -> PDFMetadata:
        """
        Extraer metadatos de un archivo PDF.
        
        Args:
            file_path: Ruta del archivo PDF
            
        Returns:
            Metadatos extraídos
        """
        file_path = Path(file_path)
        
        # Validaciones básicas
        if not file_path.exists():
            return PDFMetadata(
                file_path=file_path,
                file_size=0,
                file_hash="",
                modified_time=datetime.now(timezone.utc),
                status=PDFStatus.NOT_FOUND,
                error_message="Archivo no encontrado"
            )
            
        if not file_path.is_file():
            return PDFMetadata(
                file_path=file_path,
                file_size=0,
                file_hash="",
                modified_time=datetime.now(timezone.utc),
                status=PDFStatus.NOT_FOUND,
                error_message="No es un archivo válido"
            )
            
        # Verificar permisos
        if not os.access(file_path, os.R_OK):
            return PDFMetadata(
                file_path=file_path,
                file_size=file_path.stat().st_size,
                file_hash="",
                modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc),
                status=PDFStatus.PERMISSION_DENIED,
                error_message="Sin permisos de lectura"
            )
        
        # Seleccionar librería
        library = self._select_library(file_path)
        if not library:
            return PDFMetadata(
                file_path=file_path,
                file_size=file_path.stat().st_size,
                file_hash=calculate_file_hash(file_path),
                modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc),
                status=PDFStatus.UNSUPPORTED,
                error_message="No hay librerías PDF disponibles"
            )
        
        logger.debug(f"Extrayendo metadatos de {file_path} usando {library.value}")
        
        # Extraer según librería seleccionada
        if library == PDFLibrary.PYMUPDF:
            return self._extract_with_pymupdf(file_path)
        elif library == PDFLibrary.PDFPLUMBER:
            return self._extract_with_pdfplumber(file_path)
        elif library == PDFLibrary.PYPDFIUM2:
            return self._extract_with_pypdfium2(file_path)
        else:
            return PDFMetadata(
                file_path=file_path,
                file_size=file_path.stat().st_size,
                file_hash=calculate_file_hash(file_path),
                modified_time=datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc),
                status=PDFStatus.UNSUPPORTED,
                error_message=f"Librería no soportada: {library}"
            )


class PDFIngestor:
    """
    Gestor principal de ingesta de PDFs.
    
    Maneja el procesamiento por lotes, cache de metadatos y detección de cambios.
    """
    
    def __init__(self, base_path: Optional[Union[str, Path]] = None):
        """
        Inicializar ingestor.
        
        Args:
            base_path: Directorio base del proyecto
        """
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.reader = PDFReader()
        self.cache_file = self.base_path / "out" / "logs" / "pdf_metadata_cache.json"
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._load_cache()
        
    def _load_cache(self) -> None:
        """Cargar cache de metadatos desde disco."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
                logger.debug(f"Cache cargado: {len(self._cache)} entradas")
            except Exception as e:
                logger.warning(f"Error cargando cache de metadatos: {e}")
                self._cache = {}
        else:
            logger.debug("No existe cache previo de metadatos")
            
    def _save_cache(self) -> None:
        """Guardar cache de metadatos a disco."""
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, indent=2, ensure_ascii=False)
            logger.debug(f"Cache guardado: {len(self._cache)} entradas")
        except Exception as e:
            logger.error(f"Error guardando cache de metadatos: {e}")
            
    def _is_cached_valid(self, file_path: Path) -> bool:
        """
        Verificar si la entrada del cache es válida.
        
        Args:
            file_path: Ruta del archivo
            
        Returns:
            True si el cache es válido
        """
        file_key = str(file_path)
        
        if file_key not in self._cache:
            return False
            
        cached_data = self._cache[file_key]
        
        # Verificar que el archivo no haya cambiado
        try:
            current_size = file_path.stat().st_size
            current_mtime = file_path.stat().st_mtime
            
            cached_size = cached_data.get('file_size', 0)
            cached_mtime = cached_data.get('modified_time')
            
            if cached_mtime:
                # Convertir ISO string a timestamp para comparar
                cached_mtime_dt = datetime.fromisoformat(cached_mtime)
                cached_timestamp = cached_mtime_dt.timestamp()
                
                return (current_size == cached_size and 
                       abs(current_mtime - cached_timestamp) < 1.0)  # Tolerancia de 1 segundo
            
        except Exception as e:
            logger.debug(f"Error verificando validez del cache para {file_path}: {e}")
            
        return False
        
    def ingest_single_pdf(self, file_path: Union[str, Path], 
                         force_refresh: bool = False) -> PDFMetadata:
        """
        Ingestar un archivo PDF individual.
        
        Args:
            file_path: Ruta del archivo PDF
            force_refresh: Si forzar actualización ignorando cache
            
        Returns:
            Metadatos extraídos
        """
        file_path = Path(file_path)
        file_key = str(file_path)
        
        # Verificar cache si no se fuerza actualización
        if not force_refresh and self._is_cached_valid(file_path):
            logger.debug(f"Usando metadatos cacheados para {file_path}")
            cached_data = self._cache[file_key]
            return PDFMetadata.from_dict(cached_data)
        
        # Extraer metadatos
        with logger.timer(f"extraccion_metadatos_{file_path.name}"):
            metadata = self.reader.extract_metadata(file_path)
            
        # Guardar en cache si es exitoso
        if metadata.status == PDFStatus.VALID:
            self._cache[file_key] = metadata.to_dict()
            logger.info(f"PDF ingestado exitosamente: {file_path.name}",
                       pages=metadata.page_count,
                       size_mb=round(metadata.file_size / 1024 / 1024, 2),
                       has_text=metadata.has_text,
                       has_images=metadata.has_images)
        else:
            logger.warning(f"Error ingestando PDF: {file_path.name}",
                          status=metadata.status.value,
                          error=metadata.error_message)
            
        return metadata
        
    def ingest_directory(self, directory: Union[str, Path],
                        recursive: bool = True,
                        file_pattern: str = "*.pdf",
                        force_refresh: bool = False) -> List[PDFMetadata]:
        """
        Ingestar todos los PDFs de un directorio.
        
        Args:
            directory: Directorio a procesar
            recursive: Si buscar recursivamente
            file_pattern: Patrón de archivos a incluir
            force_refresh: Si forzar actualización de todos
            
        Returns:
            Lista de metadatos extraídos
        """
        directory = Path(directory)
        
        if not directory.exists():
            logger.error(f"Directorio no existe: {directory}")
            return []
            
        # Buscar archivos PDF
        if recursive:
            pdf_files = list(directory.rglob(file_pattern))
        else:
            pdf_files = list(directory.glob(file_pattern))
            
        pdf_files = [f for f in pdf_files if f.is_file()]
        
        if not pdf_files:
            logger.warning(f"No se encontraron archivos PDF en {directory}")
            return []
            
        logger.info(f"Iniciando ingesta de {len(pdf_files)} archivos PDF desde {directory}")
        
        results = []
        processed = 0
        errors = 0
        
        with logger.processing_context(operation="ingest_directory", pdf_count=len(pdf_files)):
            for pdf_file in pdf_files:
                try:
                    with logger.processing_context(pdf_file=pdf_file.name):
                        metadata = self.ingest_single_pdf(pdf_file, force_refresh)
                        results.append(metadata)
                        
                        if metadata.status == PDFStatus.VALID:
                            processed += 1
                        else:
                            errors += 1
                            
                except Exception as e:
                    logger.exception(f"Error procesando {pdf_file}", error=str(e))
                    errors += 1
                    
                    # Crear metadata de error
                    error_metadata = PDFMetadata(
                        file_path=pdf_file,
                        file_size=pdf_file.stat().st_size if pdf_file.exists() else 0,
                        file_hash="",
                        modified_time=datetime.now(timezone.utc),
                        status=PDFStatus.CORRUPTED,
                        error_message=str(e)
                    )
                    results.append(error_metadata)
        
        # Guardar cache actualizado
        self._save_cache()
        
        logger.info(f"Ingesta completada",
                   total_files=len(pdf_files),
                   processed=processed,
                   errors=errors,
                   success_rate=round(processed / len(pdf_files) * 100, 1) if pdf_files else 0)
        
        return results
        
    def get_ingestion_summary(self, metadata_list: List[PDFMetadata]) -> Dict[str, Any]:
        """
        Generar resumen de ingesta.
        
        Args:
            metadata_list: Lista de metadatos a resumir
            
        Returns:
            Diccionario con estadísticas de resumen
        """
        if not metadata_list:
            return {'total_files': 0}
            
        # Estadísticas básicas
        total_files = len(metadata_list)
        valid_files = [m for m in metadata_list if m.status == PDFStatus.VALID]
        error_files = [m for m in metadata_list if m.status != PDFStatus.VALID]
        
        # Estadísticas de contenido (solo archivos válidos)
        if valid_files:
            total_pages = sum(m.page_count for m in valid_files)
            total_size = sum(m.file_size for m in valid_files)
            files_with_text = sum(1 for m in valid_files if m.has_text)
            files_with_images = sum(1 for m in valid_files if m.has_images)
            
            # Estadísticas por estado
            status_counts = {}
            for metadata in metadata_list:
                status = metadata.status.value
                status_counts[status] = status_counts.get(status, 0) + 1
                
            # Estadísticas por librería usada
            library_counts = {}
            for metadata in valid_files:
                if metadata.library_used:
                    lib = metadata.library_used.value
                    library_counts[lib] = library_counts.get(lib, 0) + 1
        else:
            total_pages = 0
            total_size = 0
            files_with_text = 0
            files_with_images = 0
            status_counts = {'corrupted': len(error_files)}
            library_counts = {}
        
        return {
            'total_files': total_files,
            'valid_files': len(valid_files),
            'error_files': len(error_files),
            'success_rate': round(len(valid_files) / total_files * 100, 1) if total_files > 0 else 0,
            'total_pages': total_pages,
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / 1024 / 1024, 2),
            'files_with_text': files_with_text,
            'files_with_images': files_with_images,
            'avg_pages_per_file': round(total_pages / len(valid_files), 1) if valid_files else 0,
            'status_distribution': status_counts,
            'library_distribution': library_counts,
            'ingestion_timestamp': datetime.now(timezone.utc).isoformat()
        }
        
    def clear_cache(self, file_pattern: Optional[str] = None) -> int:
        """
        Limpiar cache de metadatos.
        
        Args:
            file_pattern: Patrón de archivos a limpiar (None para todo)
            
        Returns:
            Número de entradas eliminadas
        """
        if file_pattern is None:
            # Limpiar todo el cache
            cleared_count = len(self._cache)
            self._cache.clear()
        else:
            # Limpiar entradas que coincidan con el patrón
            import fnmatch
            to_remove = []
            for file_path in self._cache.keys():
                if fnmatch.fnmatch(file_path, file_pattern):
                    to_remove.append(file_path)
                    
            cleared_count = len(to_remove)
            for file_path in to_remove:
                del self._cache[file_path]
        
        self._save_cache()
        logger.info(f"Cache limpiado: {cleared_count} entradas eliminadas")
        return cleared_count
        
    def export_metadata(self, output_file: Union[str, Path],
                       metadata_list: List[PDFMetadata]) -> bool:
        """
        Exportar metadatos a archivo JSON.
        
        Args:
            output_file: Archivo de destino
            metadata_list: Lista de metadatos a exportar
            
        Returns:
            True si se exportó exitosamente
        """
        try:
            output_file = Path(output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            export_data = {
                'export_timestamp': datetime.now(timezone.utc).isoformat(),
                'total_files': len(metadata_list),
                'summary': self.get_ingestion_summary(metadata_list),
                'metadata': [m.to_dict() for m in metadata_list]
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Metadatos exportados a {output_file}",
                       files_exported=len(metadata_list))
            return True
            
        except Exception as e:
            logger.error(f"Error exportando metadatos a {output_file}: {e}")
            return False


# Instancia global para uso conveniente
_global_ingestor: Optional[PDFIngestor] = None


def get_pdf_ingestor(base_path: Optional[Union[str, Path]] = None) -> PDFIngestor:
    """Obtener instancia global del ingestor PDF."""
    global _global_ingestor
    if _global_ingestor is None:
        _global_ingestor = PDFIngestor(base_path)
    return _global_ingestor


def ingest_pdf(file_path: Union[str, Path], 
               force_refresh: bool = False) -> PDFMetadata:
    """Función de conveniencia para ingestar un PDF individual."""
    return get_pdf_ingestor().ingest_single_pdf(file_path, force_refresh)


def ingest_directory(directory: Union[str, Path], **kwargs) -> List[PDFMetadata]:
    """Función de conveniencia para ingestar directorio de PDFs."""
    return get_pdf_ingestor().ingest_directory(directory, **kwargs)


if __name__ == "__main__":
    # Modo de prueba/diagnóstico
    from .logging_setup import setup_logging
    from .utils_fs import get_file_manager
    
    setup_logging(level="DEBUG")
    
    # Inicializar estructura de directorios
    fm = get_file_manager()
    fm.initialize()
    
    print("=== Prueba de ingesta de PDFs ===\n")
    
    # Crear ingestor
    ingestor = PDFIngestor()
    
    # Verificar librerías disponibles
    reader = PDFReader()
    print(f"Librerías PDF disponibles: {[lib.value for lib in reader.available_libraries]}")
    
    # Buscar PDFs en directorio de entrada
    pdf_files = fm.get_input_pdfs()
    if pdf_files:
        print(f"\nPDFs encontrados: {len(pdf_files)}")
        
        # Ingestar todos los PDFs
        metadata_list = ingestor.ingest_directory(fm.structure.input_dir)
        
        # Mostrar resumen
        summary = ingestor.get_ingestion_summary(metadata_list)
        print(f"\n=== Resumen de ingesta ===")
        print(f"Total de archivos: {summary['total_files']}")
        print(f"Archivos válidos: {summary['valid_files']}")
        print(f"Archivos con errores: {summary['error_files']}")
        print(f"Tasa de éxito: {summary['success_rate']}%")
        print(f"Total de páginas: {summary['total_pages']}")
        print(f"Tamaño total: {summary['total_size_mb']} MB")
        
        if summary['status_distribution']:
            print(f"\nDistribución por estado:")
            for status, count in summary['status_distribution'].items():
                print(f"  {status}: {count}")
                
        if summary['library_distribution']:
            print(f"\nDistribución por librería:")
            for library, count in summary['library_distribution'].items():
                print(f"  {library}: {count}")
        
        # Exportar metadatos
        export_file = fm.structure.logs_dir / "pdf_metadata.json"
        if ingestor.export_metadata(export_file, metadata_list):
            print(f"\nMetadatos exportados a: {export_file}")
            
    else:
        print("\nNo se encontraron archivos PDF en input/pdfs/")
        print("Coloca archivos PDF en ese directorio para probar la ingesta")
        
        # Mostrar información de librerías disponibles
        if reader.available_libraries:
            print(f"\nLibrerías PDF listas para usar:")
            for lib in reader.available_libraries:
                print(f"  ✓ {lib.value}")
        else:
            print("\n⚠️  No hay librerías PDF disponibles")
            print("Instala al menos una: pip install PyMuPDF pdfplumber pypdfium2")
    
    print("\nPrueba de ingesta completada.")