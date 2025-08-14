"""
Utilidades del sistema de archivos para ImagenesPDF.

Proporciona funciones para:
- Cálculo de hashes SHA256 (detección de cambios en PDFs)
- Gestión de directorios de salida (out/xlsx, out/csv, out/images, etc.)
- Validación de archivos PDF
- Operaciones seguras de archivos con respaldo
- Limpieza y organización de archivos
- Detección de cambios y sincronización

Usado por todos los módulos para operaciones de archivos consistentes.
"""

import os
import shutil
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple, Iterator
from datetime import datetime, timezone
import json
import tempfile
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import time

from .logging_setup import get_logger

logger = get_logger(__name__)


@dataclass
class FileInfo:
    """Información detallada de un archivo."""
    path: Path
    size: int
    modified_time: datetime
    sha256: str
    exists: bool = True
    
    @classmethod
    def from_path(cls, path: Union[str, Path]) -> Optional['FileInfo']:
        """Crear FileInfo desde ruta de archivo."""
        path = Path(path)
        if not path.exists():
            return cls(path=path, size=0, modified_time=datetime.now(timezone.utc), 
                      sha256="", exists=False)
        
        try:
            stat = path.stat()
            return cls(
                path=path,
                size=stat.st_size,
                modified_time=datetime.fromtimestamp(stat.st_mtime, timezone.utc),
                sha256=calculate_file_hash(path),
                exists=True
            )
        except Exception as e:
            logger.error(f"Error obteniendo info de archivo {path}", error=str(e))
            return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario serializable."""
        return {
            'path': str(self.path),
            'size': self.size,
            'modified_time': self.modified_time.isoformat(),
            'sha256': self.sha256,
            'exists': self.exists
        }


@dataclass
class DirectoryStructure:
    """Estructura de directorios del proyecto."""
    base_dir: Path
    input_dir: Path
    output_dir: Path
    images_dir: Path
    flat_images_dir: Path
    xlsx_dir: Path
    csv_dir: Path
    logs_dir: Path
    
    @classmethod
    def from_base(cls, base_path: Union[str, Path]) -> 'DirectoryStructure':
        """Crear estructura desde directorio base."""
        base = Path(base_path)
        return cls(
            base_dir=base,
            input_dir=base / "input" / "pdfs",
            output_dir=base / "out",
            images_dir=base / "out" / "images",
            flat_images_dir=base / "out" / "images" / "_flat",
            xlsx_dir=base / "out" / "xlsx",
            csv_dir=base / "out" / "csv",
            logs_dir=base / "out" / "logs"
        )
    
    def create_all(self) -> None:
        """Crear todos los directorios."""
        for directory in [self.input_dir, self.output_dir, self.images_dir, 
                         self.flat_images_dir, self.xlsx_dir, self.csv_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Directorio asegurado: {directory}")


def calculate_file_hash(file_path: Union[str, Path], chunk_size: int = 8192) -> str:
    """
    Calcular hash SHA256 de un archivo.
    
    Args:
        file_path: Ruta del archivo
        chunk_size: Tamaño del chunk para lectura (optimización memoria)
        
    Returns:
        Hash SHA256 en hexadecimal
    """
    sha256_hash = hashlib.sha256()
    
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error calculando hash de {file_path}", error=str(e))
        return ""


def calculate_directory_hash(directory: Union[str, Path], 
                           include_patterns: Optional[List[str]] = None,
                           exclude_patterns: Optional[List[str]] = None) -> str:
    """
    Calcular hash colectivo de archivos en directorio.
    
    Args:
        directory: Directorio a procesar
        include_patterns: Patrones de archivos a incluir (ej: ["*.pdf", "*.yaml"])
        exclude_patterns: Patrones de archivos a excluir
        
    Returns:
        Hash SHA256 combinado de todos los archivos
    """
    directory = Path(directory)
    if not directory.exists():
        return ""
    
    sha256_hash = hashlib.sha256()
    file_hashes = []
    
    for file_path in sorted(directory.rglob("*")):
        if not file_path.is_file():
            continue
            
        # Aplicar filtros si se especifican
        if include_patterns and not any(file_path.match(pattern) for pattern in include_patterns):
            continue
        if exclude_patterns and any(file_path.match(pattern) for pattern in exclude_patterns):
            continue
            
        file_hash = calculate_file_hash(file_path)
        if file_hash:
            # Incluir ruta relativa en el hash para detectar renames/moves
            relative_path = file_path.relative_to(directory)
            combined = f"{relative_path}:{file_hash}"
            file_hashes.append(combined)
    
    # Hash de todos los archivos combinados
    for file_hash in sorted(file_hashes):
        sha256_hash.update(file_hash.encode('utf-8'))
        
    return sha256_hash.hexdigest()


def is_pdf_file(file_path: Union[str, Path]) -> bool:
    """
    Verificar si un archivo es un PDF válido.
    
    Args:
        file_path: Ruta del archivo
        
    Returns:
        True si es PDF válido
    """
    file_path = Path(file_path)
    
    if not file_path.exists() or file_path.suffix.lower() != '.pdf':
        return False
    
    try:
        # Verificar signature PDF
        with open(file_path, 'rb') as f:
            header = f.read(8)
            if header.startswith(b'%PDF-'):
                return True
        return False
    except Exception:
        return False


def safe_copy_file(source: Union[str, Path], 
                   destination: Union[str, Path],
                   create_backup: bool = True) -> bool:
    """
    Copiar archivo de forma segura con respaldo opcional.
    
    Args:
        source: Archivo fuente
        destination: Archivo destino
        create_backup: Si crear backup del archivo destino existente
        
    Returns:
        True si la copia fue exitosa
    """
    source = Path(source)
    destination = Path(destination)
    
    if not source.exists():
        logger.error(f"Archivo fuente no existe: {source}")
        return False
    
    try:
        # Crear directorio destino si no existe
        destination.parent.mkdir(parents=True, exist_ok=True)
        
        # Crear backup si el archivo destino existe
        if destination.exists() and create_backup:
            backup_path = destination.with_suffix(f"{destination.suffix}.backup.{int(time.time())}")
            shutil.copy2(destination, backup_path)
            logger.debug(f"Backup creado: {backup_path}")
        
        # Copiar archivo
        shutil.copy2(source, destination)
        logger.debug(f"Archivo copiado: {source} -> {destination}")
        return True
        
    except Exception as e:
        logger.error(f"Error copiando archivo {source} -> {destination}", error=str(e))
        return False


def safe_move_file(source: Union[str, Path], 
                   destination: Union[str, Path],
                   create_backup: bool = True) -> bool:
    """
    Mover archivo de forma segura con respaldo opcional.
    
    Args:
        source: Archivo fuente
        destination: Archivo destino
        create_backup: Si crear backup del archivo destino existente
        
    Returns:
        True si el movimiento fue exitoso
    """
    if safe_copy_file(source, destination, create_backup):
        try:
            Path(source).unlink()
            logger.debug(f"Archivo movido: {source} -> {destination}")
            return True
        except Exception as e:
            logger.error(f"Error eliminando archivo fuente {source}", error=str(e))
            return False
    return False


@contextmanager
def temporary_directory(prefix: str = "imagenespdf_"):
    """
    Context manager para directorio temporal.
    
    Args:
        prefix: Prefijo para el directorio temporal
        
    Yields:
        Path del directorio temporal
    """
    temp_dir = None
    try:
        temp_dir = Path(tempfile.mkdtemp(prefix=prefix))
        logger.debug(f"Directorio temporal creado: {temp_dir}")
        yield temp_dir
    finally:
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
                logger.debug(f"Directorio temporal eliminado: {temp_dir}")
            except Exception as e:
                logger.warning(f"Error eliminando directorio temporal {temp_dir}", error=str(e))


def clean_directory(directory: Union[str, Path], 
                   max_age_days: Optional[int] = None,
                   file_patterns: Optional[List[str]] = None,
                   dry_run: bool = False) -> int:
    """
    Limpiar archivos de un directorio según criterios.
    
    Args:
        directory: Directorio a limpiar
        max_age_days: Eliminar archivos más antiguos que N días
        file_patterns: Patrones de archivos a eliminar (ej: ["*.tmp", "*.log"])
        dry_run: Si True, solo reporta qué se eliminaría sin hacerlo
        
    Returns:
        Cantidad de archivos eliminados/que se eliminarían
    """
    directory = Path(directory)
    if not directory.exists():
        return 0
    
    deleted_count = 0
    cutoff_time = None
    
    if max_age_days:
        cutoff_time = time.time() - (max_age_days * 24 * 3600)
    
    for file_path in directory.rglob("*"):
        if not file_path.is_file():
            continue
            
        should_delete = False
        
        # Verificar edad
        if cutoff_time and file_path.stat().st_mtime < cutoff_time:
            should_delete = True
            
        # Verificar patrones
        if file_patterns and any(file_path.match(pattern) for pattern in file_patterns):
            should_delete = True
            
        if should_delete:
            if dry_run:
                logger.info(f"Se eliminaría: {file_path}")
            else:
                try:
                    file_path.unlink()
                    logger.debug(f"Archivo eliminado: {file_path}")
                except Exception as e:
                    logger.error(f"Error eliminando {file_path}", error=str(e))
                    continue
            deleted_count += 1
    
    return deleted_count


def get_directory_size(directory: Union[str, Path]) -> int:
    """
    Obtener tamaño total de un directorio en bytes.
    
    Args:
        directory: Directorio a medir
        
    Returns:
        Tamaño total en bytes
    """
    directory = Path(directory)
    if not directory.exists():
        return 0
    
    total_size = 0
    for file_path in directory.rglob("*"):
        if file_path.is_file():
            try:
                total_size += file_path.stat().st_size
            except Exception:
                continue
    
    return total_size


def format_file_size(size_bytes: int) -> str:
    """
    Formatear tamaño de archivo en formato legible.
    
    Args:
        size_bytes: Tamaño en bytes
        
    Returns:
        Tamaño formateado (ej: "1.5 MB", "832 KB")
    """
    if size_bytes == 0:
        return "0 B"
    
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(size_bytes)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.1f} {units[unit_index]}"


def find_pdf_files(directory: Union[str, Path], 
                   recursive: bool = True) -> List[Path]:
    """
    Encontrar archivos PDF en directorio.
    
    Args:
        directory: Directorio a buscar
        recursive: Si buscar recursivamente en subdirectorios
        
    Returns:
        Lista de rutas de archivos PDF válidos
    """
    directory = Path(directory)
    if not directory.exists():
        return []
    
    pdf_files = []
    pattern = "**/*.pdf" if recursive else "*.pdf"
    
    for pdf_path in directory.glob(pattern):
        if pdf_path.is_file() and is_pdf_file(pdf_path):
            pdf_files.append(pdf_path)
    
    return sorted(pdf_files)


def create_file_manifest(directory: Union[str, Path], 
                        output_file: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """
    Crear manifiesto de archivos en directorio.
    
    Args:
        directory: Directorio a procesar
        output_file: Archivo donde guardar el manifiesto (opcional)
        
    Returns:
        Diccionario con el manifiesto
    """
    directory = Path(directory)
    manifest = {
        'directory': str(directory),
        'created_at': datetime.now(timezone.utc).isoformat(),
        'files': []
    }
    
    if not directory.exists():
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)
        return manifest
    
    for file_path in sorted(directory.rglob("*")):
        if file_path.is_file():
            file_info = FileInfo.from_path(file_path)
            if file_info:
                # Ruta relativa al directorio base
                relative_path = file_path.relative_to(directory)
                file_data = file_info.to_dict()
                file_data['relative_path'] = str(relative_path)
                manifest['files'].append(file_data)
    
    manifest['total_files'] = len(manifest['files'])
    manifest['total_size'] = sum(f['size'] for f in manifest['files'])
    
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        logger.info(f"Manifiesto guardado en {output_file}")
    
    return manifest


def compare_manifests(manifest1: Dict[str, Any], 
                     manifest2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Comparar dos manifiestos de archivos.
    
    Args:
        manifest1: Manifiesto base
        manifest2: Manifiesto a comparar
        
    Returns:
        Diccionario con diferencias encontradas
    """
    # Indexar archivos por ruta relativa
    files1 = {f['relative_path']: f for f in manifest1.get('files', [])}
    files2 = {f['relative_path']: f for f in manifest2.get('files', [])}
    
    comparison = {
        'added_files': [],
        'removed_files': [],
        'modified_files': [],
        'unchanged_files': []
    }
    
    # Archivos en manifest2 pero no en manifest1 (agregados)
    for path in files2.keys() - files1.keys():
        comparison['added_files'].append(files2[path])
    
    # Archivos en manifest1 pero no en manifest2 (removidos)  
    for path in files1.keys() - files2.keys():
        comparison['removed_files'].append(files1[path])
    
    # Archivos en ambos (comparar hashes)
    for path in files1.keys() & files2.keys():
        file1 = files1[path]
        file2 = files2[path]
        
        if file1['sha256'] != file2['sha256']:
            comparison['modified_files'].append({
                'path': path,
                'old': file1,
                'new': file2
            })
        else:
            comparison['unchanged_files'].append(file1)
    
    return comparison


class ProjectFileManager:
    """
    Gestor de archivos del proyecto ImagenesPDF.
    
    Centraliza operaciones de archivos y mantiene estado consistente.
    """
    
    def __init__(self, base_path: Union[str, Path]):
        self.base_path = Path(base_path)
        self.structure = DirectoryStructure.from_base(self.base_path)
        self._manifests: Dict[str, Dict[str, Any]] = {}
        
    def initialize(self) -> None:
        """Inicializar estructura de directorios."""
        logger.info("Inicializando estructura de directorios")
        self.structure.create_all()
        
    def get_input_pdfs(self) -> List[Path]:
        """Obtener lista de PDFs en directorio de entrada."""
        return find_pdf_files(self.structure.input_dir)
        
    def get_pdf_info(self, pdf_path: Union[str, Path]) -> Optional[FileInfo]:
        """Obtener información detallada de un PDF."""
        return FileInfo.from_path(pdf_path)
        
    def create_output_manifest(self, run_id: str) -> Dict[str, Any]:
        """Crear manifiesto de archivos de salida."""
        manifest_path = self.structure.logs_dir / f"output_manifest_{run_id}.json"
        return create_file_manifest(self.structure.output_dir, manifest_path)
        
    def cleanup_old_outputs(self, max_age_days: int = 7) -> int:
        """Limpiar archivos de salida antiguos."""
        logger.info(f"Limpiando archivos de salida más antiguos que {max_age_days} días")
        
        total_deleted = 0
        for directory in [self.structure.xlsx_dir, self.structure.csv_dir, 
                         self.structure.images_dir]:
            deleted = clean_directory(directory, max_age_days=max_age_days)
            total_deleted += deleted
            
        return total_deleted
        
    def get_storage_info(self) -> Dict[str, Any]:
        """Obtener información de almacenamiento del proyecto."""
        info = {
            'base_path': str(self.base_path),
            'directories': {}
        }
        
        for attr_name in ['input_dir', 'output_dir', 'images_dir', 'xlsx_dir', 'csv_dir', 'logs_dir']:
            directory = getattr(self.structure, attr_name)
            size_bytes = get_directory_size(directory)
            file_count = len(list(directory.rglob("*"))) if directory.exists() else 0
            
            info['directories'][attr_name] = {
                'path': str(directory),
                'exists': directory.exists(),
                'size_bytes': size_bytes,
                'size_formatted': format_file_size(size_bytes),
                'file_count': file_count
            }
        
        info['total_size_bytes'] = sum(d['size_bytes'] for d in info['directories'].values())
        info['total_size_formatted'] = format_file_size(info['total_size_bytes'])
        
        return info


# Instancia global para uso conveniente
_global_file_manager: Optional[ProjectFileManager] = None


def get_file_manager(base_path: Optional[Union[str, Path]] = None) -> ProjectFileManager:
    """Obtener instancia global del gestor de archivos."""
    global _global_file_manager
    if _global_file_manager is None:
        if base_path is None:
            base_path = Path.cwd()
        _global_file_manager = ProjectFileManager(base_path)
    return _global_file_manager


if __name__ == "__main__":
    # Modo de prueba/diagnóstico
    from .logging_setup import setup_logging
    
    setup_logging(level="DEBUG")
    
    # Probar funciones principales
    print("=== Prueba de utilidades de archivos ===")
    
    # Crear gestor de archivos
    fm = get_file_manager()
    fm.initialize()
    
    # Mostrar información de almacenamiento
    storage_info = fm.get_storage_info()
    print(f"\nInformación de almacenamiento:")
    print(f"Directorio base: {storage_info['base_path']}")
    print(f"Tamaño total: {storage_info['total_size_formatted']}")
    
    for dir_name, dir_info in storage_info['directories'].items():
        status = "✓" if dir_info['exists'] else "✗"
        print(f"  {status} {dir_name}: {dir_info['size_formatted']} ({dir_info['file_count']} archivos)")
    
    # Buscar PDFs
    pdfs = fm.get_input_pdfs()
    if pdfs:
        print(f"\nPDFs encontrados: {len(pdfs)}")
        for pdf in pdfs[:5]:  # Mostrar máximo 5
            info = fm.get_pdf_info(pdf)
            if info:
                print(f"  - {pdf.name}: {format_file_size(info.size)}")
    else:
        print("\nNo se encontraron archivos PDF en input/pdfs/")
        print("Tip: Coloca archivos PDF en el directorio input/pdfs/ para procesarlos")
    
    print("\nPrueba completada.")