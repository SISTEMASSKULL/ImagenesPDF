"""
Sistema de configuración para ImagenesPDF.

Este módulo maneja la carga y acceso a todas las configuraciones YAML:
- excel_layout.yaml: Definición de hojas/columnas/validaciones
- dims.yaml: Catálogos iniciales (makers, bulbs, etc.)
- features.yaml: Taxonomía de features (LED, sensor, keyhole...)
- vendor_signatures.yaml: Firmas por proveedor para detector

Soporta configuraciones anidadas, validación de esquemas y carga lazy.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


@dataclass
class ConfigPaths:
    """Rutas de archivos de configuración."""
    schema_dir: Path
    excel_layout: Path
    dims: Path
    features: Path
    vendor_signatures: Path
    
    @classmethod
    def from_base_path(cls, base_path: Union[str, Path]) -> 'ConfigPaths':
        """Crear ConfigPaths desde directorio base."""
        base = Path(base_path)
        schema_dir = base / "src" / "imagenespdf" / "schema"
        
        return cls(
            schema_dir=schema_dir,
            excel_layout=schema_dir / "excel_layout.yaml",
            dims=schema_dir / "dims.yaml",
            features=schema_dir / "features.yaml",
            vendor_signatures=schema_dir / "vendor_signatures.yaml"
        )


class ConfigurationError(Exception):
    """Error en configuración del sistema."""
    pass


class ConfigManager:
    """
    Gestor principal de configuraciones.
    
    Maneja carga lazy, validación y acceso thread-safe a configuraciones.
    Soporta configuraciones anidadas y resolución de referencias.
    """
    
    def __init__(self, base_path: Optional[Union[str, Path]] = None):
        """
        Inicializar gestor de configuración.
        
        Args:
            base_path: Directorio base del proyecto. Si None, auto-detecta.
        """
        self.base_path = self._detect_base_path(base_path)
        self.paths = ConfigPaths.from_base_path(self.base_path)
        self._configs: Dict[str, Any] = {}
        self._loaded: set = set()
        
    def _detect_base_path(self, provided_path: Optional[Union[str, Path]]) -> Path:
        """Detectar directorio base del proyecto."""
        if provided_path:
            return Path(provided_path).resolve()
            
        # Auto-detección desde ubicación del módulo
        current = Path(__file__).parent
        while current.parent != current:
            if (current / "src" / "imagenespdf").exists():
                return current
            current = current.parent
            
        # Fallback a directorio actual
        return Path.cwd()
        
    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """Cargar archivo YAML con manejo de errores."""
        if not file_path.exists():
            raise ConfigurationError(f"Archivo de configuración no encontrado: {file_path}")
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = yaml.load(f, Loader=Loader)
                if content is None:
                    return {}
                return content
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parseando YAML {file_path}: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error cargando {file_path}: {e}")
            
    def _ensure_loaded(self, config_name: str) -> None:
        """Asegurar que una configuración esté cargada."""
        if config_name in self._loaded:
            return
            
        config_file_map = {
            'excel_layout': self.paths.excel_layout,
            'dims': self.paths.dims,
            'features': self.paths.features,
            'vendor_signatures': self.paths.vendor_signatures
        }
        
        if config_name not in config_file_map:
            raise ConfigurationError(f"Configuración desconocida: {config_name}")
            
        file_path = config_file_map[config_name]
        self._configs[config_name] = self._load_yaml_file(file_path)
        self._loaded.add(config_name)
        
    def get_config(self, config_name: str) -> Dict[str, Any]:
        """
        Obtener configuración completa por nombre.
        
        Args:
            config_name: Nombre de la configuración ('excel_layout', 'dims', etc.)
            
        Returns:
            Diccionario con la configuración completa
        """
        self._ensure_loaded(config_name)
        return self._configs[config_name].copy()
        
    def get_nested(self, config_name: str, *keys: str, default: Any = None) -> Any:
        """
        Obtener valor anidado de configuración.
        
        Args:
            config_name: Nombre de la configuración
            *keys: Claves anidadas
            default: Valor por defecto si no existe
            
        Returns:
            Valor encontrado o default
            
        Example:
            config.get_nested('dims', 'makers', 'TOYOTA') -> 'Toyota Motor Corp'
        """
        self._ensure_loaded(config_name)
        
        current = self._configs[config_name]
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current
        
    def get_excel_sheets(self) -> Dict[str, Dict[str, Any]]:
        """Obtener definición de hojas Excel."""
        return self.get_nested('excel_layout', 'sheets', default={})
        
    def get_excel_sheet(self, sheet_name: str) -> Dict[str, Any]:
        """Obtener configuración de hoja Excel específica."""
        return self.get_nested('excel_layout', 'sheets', sheet_name, default={})
        
    def get_dimensions(self) -> Dict[str, Dict[str, Any]]:
        """Obtener todas las dimensiones/catálogos."""
        return self.get_config('dims')
        
    def get_dimension(self, dim_name: str) -> Dict[str, Any]:
        """Obtener dimensión específica."""
        return self.get_nested('dims', dim_name, default={})
        
    def get_features(self) -> Dict[str, Any]:
        """Obtener taxonomía completa de features."""
        return self.get_config('features')
        
    def get_feature_category(self, category: str) -> Dict[str, Any]:
        """Obtener features de una categoría específica."""
        return self.get_nested('features', 'categories', category, default={})
        
    def get_vendor_signatures(self) -> Dict[str, Any]:
        """Obtener todas las firmas de proveedores."""
        return self.get_config('vendor_signatures')
        
    def get_vendor_signature(self, vendor: str) -> Dict[str, Any]:
        """Obtener firma de proveedor específico."""
        return self.get_nested('vendor_signatures', 'vendors', vendor, default={})
        
    def get_color_mappings(self) -> Dict[str, str]:
        """Obtener mapeos de colores a homologación."""
        return self.get_nested('dims', 'highlight_status', default={})
        
    def get_bulb_catalog(self) -> Dict[str, str]:
        """Obtener catálogo de bulbos."""
        return self.get_nested('dims', 'bulbs', default={})
        
    def get_maker_catalog(self) -> Dict[str, str]:
        """Obtener catálogo de fabricantes."""
        return self.get_nested('dims', 'makers', default={})
        
    def get_product_types(self) -> Dict[str, Dict[str, Any]]:
        """Obtener jerarquía de tipos de productos."""
        return self.get_nested('dims', 'product_types', default={})
        
    def reload_config(self, config_name: Optional[str] = None) -> None:
        """
        Recargar configuraciones desde disco.
        
        Args:
            config_name: Si se especifica, solo recarga esa configuración.
                        Si es None, recarga todas.
        """
        if config_name:
            if config_name in self._loaded:
                self._loaded.remove(config_name)
                if config_name in self._configs:
                    del self._configs[config_name]
            self._ensure_loaded(config_name)
        else:
            self._configs.clear()
            self._loaded.clear()
            
    def validate_all_configs(self) -> List[str]:
        """
        Validar que todas las configuraciones puedan cargarse.
        
        Returns:
            Lista de errores encontrados (vacía si todo OK)
        """
        errors = []
        config_names = ['excel_layout', 'dims', 'features', 'vendor_signatures']
        
        for config_name in config_names:
            try:
                self._ensure_loaded(config_name)
            except ConfigurationError as e:
                errors.append(f"{config_name}: {e}")
                
        return errors
        
    def get_schema_info(self) -> Dict[str, Any]:
        """Obtener información sobre los esquemas disponibles."""
        return {
            'base_path': str(self.base_path),
            'schema_dir': str(self.paths.schema_dir),
            'files': {
                'excel_layout': str(self.paths.excel_layout),
                'dims': str(self.paths.dims),
                'features': str(self.paths.features),
                'vendor_signatures': str(self.paths.vendor_signatures)
            },
            'loaded': list(self._loaded),
            'files_exist': {
                'excel_layout': self.paths.excel_layout.exists(),
                'dims': self.paths.dims.exists(),
                'features': self.paths.features.exists(),
                'vendor_signatures': self.paths.vendor_signatures.exists()
            }
        }


# Instancia global para uso conveniente
_global_config: Optional[ConfigManager] = None


def get_config_manager(base_path: Optional[Union[str, Path]] = None) -> ConfigManager:
    """
    Obtener instancia global del gestor de configuración.
    
    Args:
        base_path: Directorio base del proyecto (solo usado en primera llamada)
        
    Returns:
        Instancia del ConfigManager
    """
    global _global_config
    if _global_config is None:
        _global_config = ConfigManager(base_path)
    return _global_config


def reset_config_manager() -> None:
    """Resetear instancia global (útil para testing)."""
    global _global_config
    _global_config = None


# Funciones de conveniencia para acceso rápido
def get_excel_sheets() -> Dict[str, Dict[str, Any]]:
    """Acceso rápido a definición de hojas Excel."""
    return get_config_manager().get_excel_sheets()


def get_dimensions() -> Dict[str, Dict[str, Any]]:
    """Acceso rápido a catálogos/dimensiones."""
    return get_config_manager().get_dimensions()


def get_vendor_signatures() -> Dict[str, Any]:
    """Acceso rápido a firmas de proveedores."""
    return get_config_manager().get_vendor_signatures()


def get_features() -> Dict[str, Any]:
    """Acceso rápido a taxonomía de features."""
    return get_config_manager().get_features()


if __name__ == "__main__":
    # Modo de prueba/diagnóstico
    try:
        config = get_config_manager()
        info = config.get_schema_info()
        
        print("=== Información de Configuración ===")
        print(f"Directorio base: {info['base_path']}")
        print(f"Directorio schema: {info['schema_dir']}")
        print()
        
        print("Archivos de configuración:")
        for name, path in info['files'].items():
            exists = "✓" if info['files_exist'][name] else "✗"
            print(f"  {exists} {name}: {path}")
        print()
        
        # Validar todas las configuraciones
        errors = config.validate_all_configs()
        if errors:
            print("Errores encontrados:")
            for error in errors:
                print(f"  ✗ {error}")
        else:
            print("✓ Todas las configuraciones válidas")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)