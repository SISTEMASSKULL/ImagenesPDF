"""
Lógica de manejo y expansión de años para ImagenesPDF.

Implementa las reglas específicas de la industria automotriz:
- Años abreviados: '00–'29 → 2000–2029; '30–'99 → 1930–1999
- Expansión de rangos: [2003–2008] → 2003,2004,2005,2006,2007,2008
- Validación de rangos históricos automotrices (1885-actualidad)
- Normalización de formatos diversos de entrada
- Soporte para años individuales, rangos, listas mixtas

Usado por adaptadores para normalizar información de compatibilidad vehicular.
"""

import re
from typing import List, Set, Optional, Union, Tuple, Dict, Any
from datetime import datetime, date
from dataclasses import dataclass
from enum import Enum

from .logging_setup import get_logger

logger = get_logger(__name__)


class YearFormat(Enum):
    """Tipos de formato de año detectados."""
    FULL_YEAR = "full"           # 2010, 2015
    SHORT_YEAR = "short"         # 10, 15, '10, '15
    RANGE_FULL = "range_full"    # 2010-2015, 2010~2015
    RANGE_SHORT = "range_short"  # 10-15, '10-'15
    RANGE_MIXED = "range_mixed"  # 10-2015, '10-2015
    LIST_MIXED = "list_mixed"    # 2010,2012,2015 o 10,12,15
    INVALID = "invalid"


@dataclass
class YearRange:
    """Representa un rango de años válido."""
    start_year: int
    end_year: int
    original_text: str
    format_detected: YearFormat
    
    def __post_init__(self):
        """Validar rango después de inicialización."""
        if self.start_year > self.end_year:
            self.start_year, self.end_year = self.end_year, self.start_year
            
    def expand(self) -> List[int]:
        """Expandir rango a lista de años individuales."""
        return list(range(self.start_year, self.end_year + 1))
        
    def contains(self, year: int) -> bool:
        """Verificar si el rango contiene un año específico."""
        return self.start_year <= year <= self.end_year
        
    def overlaps(self, other: 'YearRange') -> bool:
        """Verificar si hay solapamiento con otro rango."""
        return not (self.end_year < other.start_year or other.end_year < self.start_year)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario para serialización."""
        return {
            'start_year': self.start_year,
            'end_year': self.end_year,
            'original_text': self.original_text,
            'format_detected': self.format_detected.value,
            'expanded_years': self.expand()
        }


class YearProcessor:
    """
    Procesador principal de años para catálogos de autopartes.
    
    Maneja todas las variantes de formato encontradas en PDFs de proveedores.
    """
    
    # Configuración de límites históricos automotrices
    MIN_AUTOMOTIVE_YEAR = 1885  # Primer automóvil de Benz
    MAX_FUTURE_YEAR = datetime.now().year + 5  # Hasta 5 años en el futuro
    
    # Patrones de reconocimiento
    PATTERNS = {
        # Años completos individuales: 2010, 2015
        'full_single': re.compile(r'\b(19[0-9]{2}|20[0-9]{2})\b'),
        
        # Años cortos individuales: 10, 15, '10, '15
        'short_single': re.compile(r"'?(\d{2})\b"),
        
        # Rangos completos: 2010-2015, 2010~2015, 2010/2015
        'range_full': re.compile(r'\b(19[0-9]{2}|20[0-9]{2})\s*[-~\/]\s*(19[0-9]{2}|20[0-9]{2})\b'),
        
        # Rangos cortos: 10-15, '10-'15, 10~15
        'range_short': re.compile(r"'?(\d{2})\s*[-~\/]\s*'?(\d{2})\b"),
        
        # Rangos mixtos: 10-2015, '10-2015
        'range_mixed': re.compile(r"'?(\d{2})\s*[-~\/]\s*(19[0-9]{2}|20[0-9]{2})\b"),
        
        # Listas separadas por comas: 2010,2012,2015 o 10,12,15
        'list_comma': re.compile(r'\b(?:(?:19|20)[0-9]{2}|\'?\d{2})(?:\s*,\s*(?:(?:19|20)[0-9]{2}|\'?\d{2}))+\b'),
        
        # Patrones específicos por proveedor
        'depo_format': re.compile(r'[\[\(]\s*([0-9\-\~\,\'\s]+)\s*[\]\)]'),  # [2003-2008], (10-15)
        'yuto_format': re.compile(r'([0-9\-\~\,\'\s]+)(?:\s+type|\s+model|\s+year)', re.IGNORECASE),
        'hushan_format': re.compile(r'(?:year|yr|model)\s*:?\s*([0-9\-\~\,\'\s]+)', re.IGNORECASE),
    }
    
    def __init__(self):
        """Inicializar procesador de años."""
        self.current_year = datetime.now().year
        self._cache: Dict[str, List[YearRange]] = {}
        
    def convert_short_year(self, short_year: int) -> int:
        """
        Convertir año abreviado a año completo según reglas de la industria.
        
        Reglas:
        - '00-'29 → 2000-2029
        - '30-'99 → 1930-1999
        
        Args:
            short_year: Año de 2 dígitos (0-99)
            
        Returns:
            Año completo de 4 dígitos
        """
        if short_year <= 29:
            return 2000 + short_year
        else:
            return 1900 + short_year
            
    def is_valid_automotive_year(self, year: int) -> bool:
        """
        Validar que un año esté en el rango automotriz válido.
        
        Args:
            year: Año a validar
            
        Returns:
            True si está en rango válido
        """
        return self.MIN_AUTOMOTIVE_YEAR <= year <= self.MAX_FUTURE_YEAR
        
    def detect_year_format(self, text: str) -> YearFormat:
        """
        Detectar formato de año en texto.
        
        Args:
            text: Texto a analizar
            
        Returns:
            Formato detectado
        """
        text = text.strip()
        
        # Verificar cada patrón en orden de especificidad
        if self.PATTERNS['range_full'].search(text):
            return YearFormat.RANGE_FULL
        elif self.PATTERNS['range_mixed'].search(text):
            return YearFormat.RANGE_MIXED  
        elif self.PATTERNS['range_short'].search(text):
            return YearFormat.RANGE_SHORT
        elif self.PATTERNS['list_comma'].search(text):
            return YearFormat.LIST_MIXED
        elif self.PATTERNS['full_single'].search(text):
            return YearFormat.FULL_YEAR
        elif self.PATTERNS['short_single'].search(text):
            return YearFormat.SHORT_YEAR
        else:
            return YearFormat.INVALID
            
    def parse_single_year(self, text: str) -> Optional[int]:
        """
        Parsear año individual.
        
        Args:
            text: Texto con año individual
            
        Returns:
            Año parseado o None si es inválido
        """
        text = text.strip().replace("'", "")
        
        try:
            year_int = int(text)
            
            # Si es de 2 dígitos, convertir
            if 0 <= year_int <= 99:
                year_int = self.convert_short_year(year_int)
            
            # Validar rango
            if self.is_valid_automotive_year(year_int):
                return year_int
            else:
                logger.warning(f"Año fuera de rango automotriz válido: {year_int}")
                return None
                
        except ValueError:
            logger.warning(f"No se pudo parsear año: {text}")
            return None
            
    def parse_year_range(self, start_text: str, end_text: str, original: str) -> Optional[YearRange]:
        """
        Parsear rango de años.
        
        Args:
            start_text: Texto del año inicial
            end_text: Texto del año final  
            original: Texto original completo
            
        Returns:
            YearRange parseado o None si es inválido
        """
        start_year = self.parse_single_year(start_text)
        end_year = self.parse_single_year(end_text)
        
        if start_year is None or end_year is None:
            return None
            
        # Detectar formato
        if len(start_text.replace("'", "")) == 2 and len(end_text.replace("'", "")) == 2:
            format_type = YearFormat.RANGE_SHORT
        elif len(start_text.replace("'", "")) == 2 and len(end_text) == 4:
            format_type = YearFormat.RANGE_MIXED
        else:
            format_type = YearFormat.RANGE_FULL
            
        return YearRange(
            start_year=start_year,
            end_year=end_year,
            original_text=original,
            format_detected=format_type
        )
        
    def parse_year_list(self, text: str) -> List[YearRange]:
        """
        Parsear lista de años separados por comas.
        
        Args:
            text: Texto con lista de años
            
        Returns:
            Lista de YearRange (cada año individual como rango de 1)
        """
        ranges = []
        parts = [part.strip() for part in text.split(',')]
        
        for part in parts:
            if not part:
                continue
                
            year = self.parse_single_year(part)
            if year is not None:
                year_range = YearRange(
                    start_year=year,
                    end_year=year,
                    original_text=part,
                    format_detected=YearFormat.FULL_YEAR if len(part.replace("'", "")) == 4 else YearFormat.SHORT_YEAR
                )
                ranges.append(year_range)
                
        return ranges
        
    def extract_years_from_text(self, text: str, 
                               vendor_specific: Optional[str] = None) -> List[YearRange]:
        """
        Extraer todos los años/rangos de un texto.
        
        Args:
            text: Texto a procesar
            vendor_specific: Proveedor específico para usar patrones especiales
            
        Returns:
            Lista de rangos de años encontrados
        """
        if not text or not text.strip():
            return []
            
        # Usar cache si está disponible
        cache_key = f"{text}:{vendor_specific}"
        if cache_key in self._cache:
            return self._cache[cache_key].copy()
            
        ranges = []
        text = text.strip()
        
        # Aplicar patrón específico del proveedor primero
        if vendor_specific:
            pattern_key = f"{vendor_specific.lower()}_format"
            if pattern_key in self.PATTERNS:
                matches = self.PATTERNS[pattern_key].findall(text)
                for match in matches:
                    # Procesar el contenido extraído recursivamente
                    sub_ranges = self.extract_years_from_text(match)
                    ranges.extend(sub_ranges)
                
                # Si encontramos algo con patrón específico, usar eso
                if ranges:
                    self._cache[cache_key] = ranges
                    return ranges.copy()
        
        # Procesar según formato detectado
        format_type = self.detect_year_format(text)
        
        if format_type == YearFormat.RANGE_FULL:
            match = self.PATTERNS['range_full'].search(text)
            if match:
                year_range = self.parse_year_range(match.group(1), match.group(2), match.group(0))
                if year_range:
                    ranges.append(year_range)
                    
        elif format_type == YearFormat.RANGE_SHORT:
            match = self.PATTERNS['range_short'].search(text)
            if match:
                year_range = self.parse_year_range(match.group(1), match.group(2), match.group(0))
                if year_range:
                    ranges.append(year_range)
                    
        elif format_type == YearFormat.RANGE_MIXED:
            match = self.PATTERNS['range_mixed'].search(text)
            if match:
                year_range = self.parse_year_range(match.group(1), match.group(2), match.group(0))
                if year_range:
                    ranges.append(year_range)
                    
        elif format_type == YearFormat.LIST_MIXED:
            match = self.PATTERNS['list_comma'].search(text)
            if match:
                list_ranges = self.parse_year_list(match.group(0))
                ranges.extend(list_ranges)
                
        elif format_type in [YearFormat.FULL_YEAR, YearFormat.SHORT_YEAR]:
            # Buscar todos los años individuales
            if format_type == YearFormat.FULL_YEAR:
                matches = self.PATTERNS['full_single'].findall(text)
            else:
                matches = self.PATTERNS['short_single'].findall(text)
                
            for match in matches:
                year = self.parse_single_year(match)
                if year is not None:
                    year_range = YearRange(
                        start_year=year,
                        end_year=year,
                        original_text=match,
                        format_detected=format_type
                    )
                    ranges.append(year_range)
        
        # Cache y retorna
        self._cache[cache_key] = ranges
        return ranges.copy()
        
    def expand_all_years(self, text: str, 
                        vendor_specific: Optional[str] = None) -> List[int]:
        """
        Expandir todos los años/rangos de un texto a lista de años individuales.
        
        Args:
            text: Texto a procesar
            vendor_specific: Proveedor específico
            
        Returns:
            Lista ordenada y única de años individuales
        """
        ranges = self.extract_years_from_text(text, vendor_specific)
        all_years = set()
        
        for year_range in ranges:
            all_years.update(year_range.expand())
            
        return sorted(list(all_years))
        
    def consolidate_ranges(self, ranges: List[YearRange]) -> List[YearRange]:
        """
        Consolidar rangos solapados o adyacentes.
        
        Args:
            ranges: Lista de rangos a consolidar
            
        Returns:
            Lista consolidada de rangos
        """
        if not ranges:
            return []
            
        # Ordenar por año de inicio
        sorted_ranges = sorted(ranges, key=lambda r: r.start_year)
        consolidated = [sorted_ranges[0]]
        
        for current in sorted_ranges[1:]:
            last = consolidated[-1]
            
            # Si son adyacentes o solapados, consolidar
            if current.start_year <= last.end_year + 1:
                # Expandir el rango existente
                consolidated[-1] = YearRange(
                    start_year=last.start_year,
                    end_year=max(last.end_year, current.end_year),
                    original_text=f"{last.original_text}, {current.original_text}",
                    format_detected=YearFormat.RANGE_FULL
                )
            else:
                # Agregar como rango separado
                consolidated.append(current)
                
        return consolidated
        
    def validate_year_compatibility(self, vehicle_years: List[int], 
                                   part_years: List[int]) -> Dict[str, Any]:
        """
        Validar compatibilidad entre años de vehículo y parte.
        
        Args:
            vehicle_years: Años del vehículo
            part_years: Años de la parte
            
        Returns:
            Diccionario con información de compatibilidad
        """
        vehicle_set = set(vehicle_years)
        part_set = set(part_years)
        
        compatible_years = vehicle_set & part_set
        vehicle_only = vehicle_set - part_set
        part_only = part_set - vehicle_set
        
        return {
            'compatible_years': sorted(list(compatible_years)),
            'vehicle_only_years': sorted(list(vehicle_only)),
            'part_only_years': sorted(list(part_only)),
            'compatibility_ratio': len(compatible_years) / len(vehicle_set) if vehicle_set else 0,
            'is_fully_compatible': len(vehicle_only) == 0,
            'has_extra_coverage': len(part_only) > 0
        }
        
    def get_decade_summary(self, years: List[int]) -> Dict[str, Any]:
        """
        Obtener resumen por década de una lista de años.
        
        Args:
            years: Lista de años
            
        Returns:
            Diccionario con estadísticas por década
        """
        if not years:
            return {}
            
        decades = {}
        for year in years:
            decade = (year // 10) * 10
            decade_key = f"{decade}s"
            
            if decade_key not in decades:
                decades[decade_key] = {
                    'decade_start': decade,
                    'years': [],
                    'count': 0,
                    'year_range': {'min': year, 'max': year}
                }
            
            decades[decade_key]['years'].append(year)
            decades[decade_key]['count'] += 1
            decades[decade_key]['year_range']['min'] = min(decades[decade_key]['year_range']['min'], year)
            decades[decade_key]['year_range']['max'] = max(decades[decade_key]['year_range']['max'], year)
        
        # Ordenar años dentro de cada década
        for decade_info in decades.values():
            decade_info['years'].sort()
            
        return decades
        
    def clear_cache(self) -> None:
        """Limpiar cache de procesamiento."""
        self._cache.clear()
        
    def get_cache_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas del cache."""
        return {
            'cached_entries': len(self._cache),
            'memory_usage_estimate': sum(len(str(k)) + len(str(v)) for k, v in self._cache.items())
        }


# Instancia global para uso conveniente
_global_year_processor: Optional[YearProcessor] = None


def get_year_processor() -> YearProcessor:
    """Obtener instancia global del procesador de años."""
    global _global_year_processor
    if _global_year_processor is None:
        _global_year_processor = YearProcessor()
    return _global_year_processor


def expand_years(text: str, vendor_specific: Optional[str] = None) -> List[int]:
    """
    Función de conveniencia para expandir años desde texto.
    
    Args:
        text: Texto con años/rangos
        vendor_specific: Proveedor específico (opcional)
        
    Returns:
        Lista de años individuales
    """
    return get_year_processor().expand_all_years(text, vendor_specific)


def parse_years(text: str, vendor_specific: Optional[str] = None) -> List[YearRange]:
    """
    Función de conveniencia para parsear rangos de años.
    
    Args:
        text: Texto con años/rangos
        vendor_specific: Proveedor específico (opcional)
        
    Returns:
        Lista de rangos de años
    """
    return get_year_processor().extract_years_from_text(text, vendor_specific)


def convert_short_year(short_year: int) -> int:
    """
    Función de conveniencia para convertir año corto.
    
    Args:
        short_year: Año de 2 dígitos
        
    Returns:
        Año de 4 dígitos
    """
    return get_year_processor().convert_short_year(short_year)


if __name__ == "__main__":
    # Modo de prueba/diagnóstico
    from .logging_setup import setup_logging
    
    setup_logging(level="DEBUG")
    
    processor = YearProcessor()
    
    # Casos de prueba
    test_cases = [
        # Años individuales
        "2010",
        "15",
        "'15",
        
        # Rangos completos
        "2010-2015",
        "2010~2015", 
        "2010/2015",
        
        # Rangos cortos
        "10-15",
        "'10-'15",
        "10~15",
        
        # Rangos mixtos  
        "10-2015",
        "'10-2015",
        
        # Listas
        "2010,2012,2015",
        "10,12,15",
        "'10,'12,'15",
        
        # Formatos específicos de proveedores
        "[2003-2008]",
        "(10-15)",
        "Year: 2010-2015",
        "Model 10-15 type",
        
        # Casos complejos
        "Compatible with 2010,2012,2015-2018 models",
        "Fits '03-'08 and 2010+ vehicles",
        
        # Casos edge
        "",
        "No years here",
        "abc-def",
        "00-29",  # Prueba regla de siglos
        "30-99",  # Prueba regla de siglos
    ]
    
    print("=== Prueba de procesamiento de años ===\n")
    
    for i, test_text in enumerate(test_cases, 1):
        print(f"{i:2d}. Input: '{test_text}'")
        
        # Detectar formato
        format_detected = processor.detect_year_format(test_text)
        print(f"    Formato: {format_detected.value}")
        
        # Extraer rangos
        ranges = processor.extract_years_from_text(test_text)
        if ranges:
            print(f"    Rangos: {len(ranges)} encontrados")
            for j, year_range in enumerate(ranges):
                expanded = year_range.expand()
                print(f"      {j+1}. {year_range.start_year}-{year_range.end_year} → {expanded}")
        else:
            print("    Rangos: Ninguno encontrado")
            
        # Expandir todos
        expanded_years = processor.expand_all_years(test_text)
        if expanded_years:
            print(f"    Expandido: {expanded_years}")
        else:
            print("    Expandido: []")
            
        print()
    
    # Mostrar estadísticas del cache
    cache_stats = processor.get_cache_stats()
    print(f"Cache: {cache_stats['cached_entries']} entradas")
    
    # Prueba de consolidación
    print("\n=== Prueba de consolidación de rangos ===")
    test_ranges = [
        YearRange(2010, 2012, "2010-2012", YearFormat.RANGE_FULL),
        YearRange(2013, 2015, "2013-2015", YearFormat.RANGE_FULL),
        YearRange(2020, 2020, "2020", YearFormat.FULL_YEAR),
        YearRange(2021, 2021, "2021", YearFormat.FULL_YEAR),
        YearRange(2025, 2028, "2025-2028", YearFormat.RANGE_FULL)
    ]
    
    consolidated = processor.consolidate_ranges(test_ranges)
    print(f"Original: {len(test_ranges)} rangos")
    print(f"Consolidado: {len(consolidated)} rangos")
    for rng in consolidated:
        print(f"  {rng.start_year}-{rng.end_year} (original: '{rng.original_text}')")
    
    print("\nPrueba completada.")