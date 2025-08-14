"""
Sistema de detección automática de proveedores para ImagenesPDF.

Analiza el contenido de PDFs para identificar automáticamente el proveedor:
- DEPO: Códigos A-J, patrones específicos de layout
- YUTO: Sufijos característicos, formato de features
- HUSHAN: Patrones de acabados, keyhole/sensor
- GENERIC: Fallback para proveedores no reconocidos

Utiliza múltiples estrategias de detección:
- Análisis de texto y patrones
- Estructura del documento
- Metadatos del PDF
- Firmas específicas por proveedor
- Machine learning básico para casos ambiguos

Integra con el sistema de configuración para firmas actualizables.
"""

import re
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Union, Set
from dataclasses import dataclass, field
from enum import Enum
import statistics
from collections import Counter, defaultdict

try:
    import fitz  # PyMuPDF para extracción de texto
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

from .logging_setup import get_logger
from .config import get_config_manager
from .ingest import PDFMetadata, PDFStatus

logger = get_logger(__name__)


class VendorType(Enum):
    """Tipos de proveedor soportados."""
    DEPO = "depo"
    YUTO = "yuto" 
    HUSHAN = "hushan"
    GENERIC = "generic"
    UNKNOWN = "unknown"


class ConfidenceLevel(Enum):
    """Niveles de confianza en la detección."""
    VERY_HIGH = "very_high"  # 90-100%
    HIGH = "high"           # 70-89%
    MEDIUM = "medium"       # 50-69%
    LOW = "low"            # 25-49%
    VERY_LOW = "very_low"  # 0-24%


@dataclass
class DetectionSignature:
    """Firma de detección para un proveedor específico."""
    
    # Patrones de texto obligatorios
    required_patterns: List[str] = field(default_factory=list)
    
    # Patrones de texto opcionales (suman puntos)
    optional_patterns: List[str] = field(default_factory=list)
    
    # Patrones que descartan este proveedor
    exclusion_patterns: List[str] = field(default_factory=list)
    
    # Patrones en metadatos
    metadata_patterns: Dict[str, List[str]] = field(default_factory=dict)
    
    # Estructura esperada del documento
    expected_structure: Dict[str, Any] = field(default_factory=dict)
    
    # Peso de esta firma (para casos ambiguos)
    weight: float = 1.0
    
    def compile_patterns(self) -> None:
        """Compilar patrones regex para optimización."""
        self._required_regex = [re.compile(pattern, re.IGNORECASE | re.MULTILINE) 
                               for pattern in self.required_patterns]
        self._optional_regex = [re.compile(pattern, re.IGNORECASE | re.MULTILINE)
                               for pattern in self.optional_patterns]
        self._exclusion_regex = [re.compile(pattern, re.IGNORECASE | re.MULTILINE)
                                for pattern in self.exclusion_patterns]


@dataclass
class DetectionResult:
    """Resultado de detección de proveedor."""
    vendor: VendorType
    confidence: float  # 0.0 - 1.0
    confidence_level: ConfidenceLevel
    matched_signatures: List[str] = field(default_factory=list)
    evidence: Dict[str, Any] = field(default_factory=dict)
    page_analysis: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    
    def __post_init__(self):
        """Calcular nivel de confianza basado en score numérico."""
        if self.confidence >= 0.90:
            self.confidence_level = ConfidenceLevel.VERY_HIGH
        elif self.confidence >= 0.70:
            self.confidence_level = ConfidenceLevel.HIGH
        elif self.confidence >= 0.50:
            self.confidence_level = ConfidenceLevel.MEDIUM
        elif self.confidence >= 0.25:
            self.confidence_level = ConfidenceLevel.LOW
        else:
            self.confidence_level = ConfidenceLevel.VERY_LOW
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario serializable."""
        return {
            'vendor': self.vendor.value,
            'confidence': self.confidence,
            'confidence_level': self.confidence_level.value,
            'matched_signatures': self.matched_signatures,
            'evidence': self.evidence,
            'page_analysis': self.page_analysis
        }


class VendorSignatureManager:
    """
    Gestor de firmas de detección de proveedores.
    
    Carga firmas desde configuración y las mantiene actualizadas.
    """
    
    def __init__(self):
        """Inicializar gestor de firmas."""
        self.config_manager = get_config_manager()
        self.signatures: Dict[VendorType, List[DetectionSignature]] = {}
        self._load_signatures()
        
    def _load_signatures(self) -> None:
        """Cargar firmas desde configuración."""
        try:
            vendor_config = self.config_manager.get_vendor_signatures()
            
            # Cargar firmas configurables
            vendors = vendor_config.get('vendors', {})
            for vendor_name, vendor_data in vendors.items():
                try:
                    vendor_type = VendorType(vendor_name.lower())
                    signatures = []
                    
                    for sig_data in vendor_data.get('signatures', []):
                        signature = DetectionSignature(
                            required_patterns=sig_data.get('required_patterns', []),
                            optional_patterns=sig_data.get('optional_patterns', []),
                            exclusion_patterns=sig_data.get('exclusion_patterns', []),
                            metadata_patterns=sig_data.get('metadata_patterns', {}),
                            expected_structure=sig_data.get('expected_structure', {}),
                            weight=sig_data.get('weight', 1.0)
                        )
                        signature.compile_patterns()
                        signatures.append(signature)
                    
                    self.signatures[vendor_type] = signatures
                    logger.debug(f"Cargadas {len(signatures)} firmas para {vendor_type.value}")
                    
                except ValueError:
                    logger.warning(f"Proveedor desconocido en configuración: {vendor_name}")
                    
        except Exception as e:
            logger.error(f"Error cargando firmas de proveedores: {e}")
            
        # Si no se pudieron cargar firmas desde config, usar firmas por defecto
        if not self.signatures:
            self._load_default_signatures()
            
    def _load_default_signatures(self) -> None:
        """Cargar firmas por defecto embebidas en código."""
        logger.info("Cargando firmas por defecto de proveedores")
        
        # Firmas DEPO
        depo_signatures = [
            DetectionSignature(
                required_patterns=[
                    r'DEPO',
                    r'[A-J]\d{2}[-\s]\d{4}[-\s][A-Z]{2,3}\d*',  # Códigos A-J
                ],
                optional_patterns=[
                    r'Taiwan',
                    r'Made\s+in\s+Taiwan',
                    r'OEM\s*:?\s*\d{3}[-\s]\d{5}[-\s][A-Z]{2}\d*',
                    r'ELEC\s*=\s*YES',
                    r'MOTOR\s*=\s*YES',
                    r'PCS\s*=\s*\d+',
                    r'CFT\s*=\s*\d+\.?\d*'
                ],
                exclusion_patterns=[
                    r'YUTO',
                    r'HUSHAN',
                    r'Made\s+in\s+China'
                ],
                weight=1.5
            )
        ]
        
        # Firmas YUTO
        yuto_signatures = [
            DetectionSignature(
                required_patterns=[
                    r'YUTO',
                    r'[LR]HD?\s+(?:LED|LAMP|LIGHT)',
                ],
                optional_patterns=[
                    r'China',
                    r'Made\s+in\s+China',
                    r'LED\s+(?:DRL|Puddle|Heat)',
                    r'USA\s+type',
                    r'Non[-\s]US',
                    r'PCS\s*:?\s*\d+',
                    r'CFT\s*:?\s*\d+\.?\d*',
                    r'N\.W\.\s*:?\s*\d+\.?\d*\s*KG',
                    r'G\.W\.\s*:?\s*\d+\.?\d*\s*KG'
                ],
                exclusion_patterns=[
                    r'DEPO',
                    r'HUSHAN',
                    r'Taiwan'
                ],
                weight=1.3
            )
        ]
        
        # Firmas HUSHAN
        hushan_signatures = [
            DetectionSignature(
                required_patterns=[
                    r'HUSHAN',
                    r'(?:Black|Chrome|Clear)\s+(?:Housing|Finish|Style)',
                ],
                optional_patterns=[
                    r'China',
                    r'with\s+keyhole',
                    r'smart\s+key\s+sensor',
                    r'heated',
                    r'mirror\s+glass',
                    r'turn\s+signal',
                    r'PCS\s*=\s*\d+',
                    r'N\.W\.\s*=\s*\d+\.?\d*\s*KG'
                ],
                exclusion_patterns=[
                    r'DEPO',
                    r'YUTO',
                    r'Taiwan'
                ],
                weight=1.2
            )
        ]
        
        # Firmas genéricas
        generic_signatures = [
            DetectionSignature(
                required_patterns=[
                    r'(?:OEM|OE)\s*:?\s*[A-Z0-9\-]+',
                    r'(?:Part|Model)\s*:?\s*[A-Z0-9\-]+',
                ],
                optional_patterns=[
                    r'Automotive',
                    r'Auto\s+Parts',
                    r'Compatible\s+with',
                    r'Fits\s+\d{4}[-\s]\d{4}',
                    r'Year\s*:?\s*\d{4}',
                ],
                exclusion_patterns=[],
                weight=0.5
            )
        ]
        
        self.signatures = {
            VendorType.DEPO: depo_signatures,
            VendorType.YUTO: yuto_signatures,
            VendorType.HUSHAN: hushan_signatures,
            VendorType.GENERIC: generic_signatures
        }
        
        # Compilar patrones
        for vendor_signatures in self.signatures.values():
            for signature in vendor_signatures:
                signature.compile_patterns()
    
    def get_signatures(self, vendor: VendorType) -> List[DetectionSignature]:
        """Obtener firmas de un proveedor específico."""
        return self.signatures.get(vendor, [])
        
    def get_all_signatures(self) -> Dict[VendorType, List[DetectionSignature]]:
        """Obtener todas las firmas cargadas."""
        return self.signatures.copy()


class TextExtractor:
    """
    Extractor de texto optimizado para diferentes librerías PDF.
    """
    
    def __init__(self, preferred_library: Optional[str] = None):
        """Inicializar extractor."""
        self.preferred_library = preferred_library
        
    def extract_text_from_pdf(self, file_path: Path, 
                             max_pages: Optional[int] = 10) -> Dict[int, str]:
        """
        Extraer texto de PDF página por página.
        
        Args:
            file_path: Ruta del archivo PDF
            max_pages: Máximo número de páginas a procesar (None = todas)
            
        Returns:
            Diccionario {página: texto}
        """
        page_texts = {}
        
        # Intentar con PyMuPDF primero (más rápido)
        if HAS_PYMUPDF:
            try:
                return self._extract_with_pymupdf(file_path, max_pages)
            except Exception as e:
                logger.warning(f"Error con PyMuPDF, intentando pdfplumber: {e}")
        
        # Fallback a pdfplumber
        if HAS_PDFPLUMBER:
            try:
                return self._extract_with_pdfplumber(file_path, max_pages)
            except Exception as e:
                logger.warning(f"Error con pdfplumber: {e}")
        
        logger.error(f"No se pudo extraer texto de {file_path}")
        return page_texts
        
    def _extract_with_pymupdf(self, file_path: Path, 
                             max_pages: Optional[int]) -> Dict[int, str]:
        """Extraer texto usando PyMuPDF."""
        page_texts = {}
        
        with fitz.open(str(file_path)) as doc:
            page_count = len(doc)
            if max_pages:
                page_count = min(page_count, max_pages)
                
            for page_num in range(page_count):
                page = doc[page_num]
                text = page.get_text()
                page_texts[page_num] = text
                
        return page_texts
        
    def _extract_with_pdfplumber(self, file_path: Path,
                                max_pages: Optional[int]) -> Dict[int, str]:
        """Extraer texto usando pdfplumber."""
        page_texts = {}
        
        with pdfplumber.open(file_path) as pdf:
            page_count = len(pdf.pages)
            if max_pages:
                page_count = min(page_count, max_pages)
                
            for page_num in range(page_count):
                page = pdf.pages[page_num]
                text = page.extract_text() or ""
                page_texts[page_num] = text
                
        return page_texts


class VendorDetector:
    """
    Detector principal de proveedores.
    
    Analiza PDFs y determina el proveedor más probable basado en múltiples criterios.
    """
    
    def __init__(self):
        """Inicializar detector."""
        self.signature_manager = VendorSignatureManager()
        self.text_extractor = TextExtractor()
        self._detection_cache: Dict[str, DetectionResult] = {}
        
    def detect_vendor(self, pdf_metadata: PDFMetadata,
                     force_refresh: bool = False) -> DetectionResult:
        """
        Detectar proveedor de un PDF.
        
        Args:
            pdf_metadata: Metadatos del PDF a analizar
            force_refresh: Si forzar re-análisis ignorando cache
            
        Returns:
            Resultado de detección
        """
        file_path = pdf_metadata.file_path
        cache_key = f"{file_path}:{pdf_metadata.file_hash}"
        
        # Verificar cache
        if not force_refresh and cache_key in self._detection_cache:
            logger.debug(f"Usando detección cacheada para {file_path.name}")
            return self._detection_cache[cache_key]
            
        logger.info(f"Detectando proveedor para {file_path.name}")
        
        # Validar PDF
        if pdf_metadata.status != PDFStatus.VALID:
            result = DetectionResult(
                vendor=VendorType.UNKNOWN,
                confidence=0.0,
                evidence={'error': f'PDF inválido: {pdf_metadata.error_message}'}
            )
            self._detection_cache[cache_key] = result
            return result
            
        with logger.timer(f"deteccion_proveedor_{file_path.name}"):
            result = self._perform_detection(pdf_metadata)
            
        # Cache resultado
        self._detection_cache[cache_key] = result
        
        logger.info(f"Proveedor detectado: {result.vendor.value}",
                   confidence=f"{result.confidence:.2f}",
                   level=result.confidence_level.value,
                   evidence_count=len(result.evidence))
        
        return result
        
    def _perform_detection(self, pdf_metadata: PDFMetadata) -> DetectionResult:
        """Realizar detección completa de proveedor."""
        file_path = pdf_metadata.file_path
        
        # Extraer texto del PDF
        page_texts = self.text_extractor.extract_text_from_pdf(file_path)
        if not page_texts:
            return DetectionResult(
                vendor=VendorType.UNKNOWN,
                confidence=0.0,
                evidence={'error': 'No se pudo extraer texto del PDF'}
            )
            
        # Combinar texto de todas las páginas para análisis global
        full_text = "\n".join(page_texts.values())
        
        # Analizar contra cada proveedor
        vendor_scores = {}
        detailed_analysis = {}
        
        for vendor, signatures in self.signature_manager.get_all_signatures().items():
            if vendor == VendorType.UNKNOWN:
                continue
                
            score, analysis = self._analyze_vendor_signatures(
                full_text, page_texts, pdf_metadata, signatures
            )
            
            vendor_scores[vendor] = score
            detailed_analysis[vendor] = analysis
            
        # Determinar mejor match
        if not vendor_scores or max(vendor_scores.values()) == 0:
            # No se encontró match específico, usar genérico
            best_vendor = VendorType.GENERIC
            confidence = 0.3  # Confianza baja para genérico
            best_analysis = detailed_analysis.get(VendorType.GENERIC, {})
        else:
            best_vendor = max(vendor_scores.keys(), key=lambda v: vendor_scores[v])
            max_score = vendor_scores[best_vendor]
            confidence = min(max_score, 1.0)
            best_analysis = detailed_analysis[best_vendor]
            
        # Crear resultado
        result = DetectionResult(
            vendor=best_vendor,
            confidence=confidence,
            matched_signatures=best_analysis.get('matched_signatures', []),
            evidence=self._compile_evidence(best_analysis, vendor_scores),
            page_analysis=best_analysis.get('page_analysis', {})
        )
        
        return result
        
    def _analyze_vendor_signatures(self, full_text: str, page_texts: Dict[int, str],
                                  pdf_metadata: PDFMetadata,
                                  signatures: List[DetectionSignature]) -> Tuple[float, Dict[str, Any]]:
        """
        Analizar texto contra firmas de un proveedor específico.
        
        Returns:
            Tupla (score, análisis_detallado)
        """
        total_score = 0.0
        matched_signatures = []
        page_analysis = {}
        signature_details = []
        
        for i, signature in enumerate(signatures):
            sig_score = 0.0
            sig_evidence = {
                'required_matches': [],
                'optional_matches': [],
                'exclusions_found': []
            }
            
            # Verificar patrones obligatorios
            required_matches = 0
            for regex in signature._required_regex:
                matches = regex.findall(full_text)
                if matches:
                    required_matches += 1
                    sig_evidence['required_matches'].extend(matches)
                    
            # Si no se cumplen los requisitos obligatorios, skip
            if required_matches < len(signature._required_regex):
                continue
                
            # Score base por cumplir requisitos
            sig_score += 0.6
            
            # Verificar patrones opcionales
            optional_matches = 0
            for regex in signature._optional_regex:
                matches = regex.findall(full_text)
                if matches:
                    optional_matches += 1
                    sig_evidence['optional_matches'].extend(matches)
                    
            # Bonus por patrones opcionales
            if signature._optional_regex:
                optional_ratio = optional_matches / len(signature._optional_regex)
                sig_score += optional_ratio * 0.3
                
            # Verificar exclusiones (penalty)
            exclusions_found = 0
            for regex in signature._exclusion_regex:
                matches = regex.findall(full_text)
                if matches:
                    exclusions_found += 1
                    sig_evidence['exclusions_found'].extend(matches)
                    
            # Penalty por exclusiones
            if exclusions_found > 0:
                sig_score *= max(0.1, 1.0 - (exclusions_found * 0.3))
                
            # Aplicar peso de la firma
            sig_score *= signature.weight
            
            # Si esta firma tiene score > 0, contarla
            if sig_score > 0:
                matched_signatures.append(f"signature_{i}")
                total_score += sig_score
                signature_details.append({
                    'signature_id': i,
                    'score': sig_score,
                    'evidence': sig_evidence
                })
        
        # Análisis por página (para primeras 5 páginas)
        for page_num in range(min(5, len(page_texts))):
            page_text = page_texts.get(page_num, "")
            page_analysis[page_num] = {
                'text_length': len(page_text),
                'has_vendor_keywords': self._count_vendor_keywords(page_text),
                'pattern_density': self._calculate_pattern_density(page_text)
            }
        
        analysis = {
            'matched_signatures': matched_signatures,
            'signature_details': signature_details,
            'page_analysis': page_analysis,
            'total_signatures': len(signatures),
            'matched_count': len(matched_signatures)
        }
        
        return total_score, analysis
        
    def _count_vendor_keywords(self, text: str) -> Dict[str, int]:
        """Contar keywords específicos de proveedores en texto."""
        keywords = {
            'depo_keywords': ['DEPO', 'Taiwan', 'ELEC', 'MOTOR', 'OEM'],
            'yuto_keywords': ['YUTO', 'China', 'LED', 'RHD', 'LHD', 'USA type'],
            'hushan_keywords': ['HUSHAN', 'keyhole', 'sensor', 'heated', 'chrome'],
            'generic_keywords': ['Part', 'Model', 'Compatible', 'Fits', 'Year']
        }
        
        counts = {}
        for category, words in keywords.items():
            count = 0
            for word in words:
                count += len(re.findall(re.escape(word), text, re.IGNORECASE))
            counts[category] = count
            
        return counts
        
    def _calculate_pattern_density(self, text: str) -> Dict[str, float]:
        """Calcular densidad de patrones característicos."""
        if not text:
            return {'overall': 0.0}
            
        text_length = len(text)
        
        patterns = {
            'part_codes': r'[A-Z]\d{2}[-\s]\d{4}[-\s][A-Z]{2,3}\d*',
            'oem_codes': r'(?:OEM|OE)\s*:?\s*[A-Z0-9\-]+',
            'measurements': r'\d+\.?\d*\s*(?:MM|CM|INCH|KG|LBS)',
            'years': r'(?:19|20)\d{2}[-\s](?:19|20)\d{2}',
            'features': r'(?:LED|DRL|heated|sensor|keyhole)',
        }
        
        densities = {}
        for pattern_name, pattern in patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            densities[pattern_name] = len(matches) / (text_length / 1000)  # Per 1000 chars
            
        densities['overall'] = statistics.mean(densities.values()) if densities else 0.0
        return densities
        
    def _compile_evidence(self, analysis: Dict[str, Any], 
                         vendor_scores: Dict[VendorType, float]) -> Dict[str, Any]:
        """Compilar evidencia de detección para el resultado final."""
        evidence = {
            'analysis_summary': {
                'matched_signatures': len(analysis.get('matched_signatures', [])),
                'total_signatures': analysis.get('total_signatures', 0),
                'match_ratio': len(analysis.get('matched_signatures', [])) / max(1, analysis.get('total_signatures', 1))
            },
            'vendor_scores': {v.value: s for v, s in vendor_scores.items()},
            'signature_details': analysis.get('signature_details', []),
            'pages_analyzed': len(analysis.get('page_analysis', {}))
        }
        
        # Agregar resumen de patrones encontrados
        all_required = []
        all_optional = []
        for detail in analysis.get('signature_details', []):
            all_required.extend(detail['evidence']['required_matches'])
            all_optional.extend(detail['evidence']['optional_matches'])
            
        if all_required or all_optional:
            evidence['patterns_found'] = {
                'required_patterns': list(set(all_required)),
                'optional_patterns': list(set(all_optional)),
                'total_pattern_matches': len(all_required) + len(all_optional)
            }
            
        return evidence
        
    def get_detection_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de detecciones realizadas."""
        if not self._detection_cache:
            return {'total_detections': 0}
            
        vendor_counts = Counter()
        confidence_levels = Counter()
        
        for result in self._detection_cache.values():
            vendor_counts[result.vendor.value] += 1
            confidence_levels[result.confidence_level.value] += 1
            
        return {
            'total_detections': len(self._detection_cache),
            'vendor_distribution': dict(vendor_counts),
            'confidence_distribution': dict(confidence_levels),
            'average_confidence': statistics.mean(r.confidence for r in self._detection_cache.values()),
            'cache_size': len(self._detection_cache)
        }
        
    def clear_cache(self) -> int:
        """Limpiar cache de detecciones."""
        cleared = len(self._detection_cache)
        self._detection_cache.clear()
        logger.info(f"Cache de detección limpiado: {cleared} entradas")
        return cleared


# Instancia global para uso conveniente  
_global_detector: Optional[VendorDetector] = None


def get_vendor_detector() -> VendorDetector:
    """Obtener instancia global del detector de proveedores."""
    global _global_detector
    if _global_detector is None:
        _global_detector = VendorDetector()
    return _global_detector


def detect_vendor(pdf_metadata: PDFMetadata, 
                 force_refresh: bool = False) -> DetectionResult:
    """Función de conveniencia para detectar proveedor."""
    return get_vendor_detector().detect_vendor(pdf_metadata, force_refresh)


if __name__ == "__main__":
    # Modo de prueba/diagnóstico
    from .logging_setup import setup_logging
    from .utils_fs import get_file_manager
    from .ingest import get_pdf_ingestor
    
    setup_logging(level="DEBUG")
    
    print("=== Prueba de detección de proveedores ===\n")
    
    # Inicializar componentes
    fm = get_file_manager()
    fm.initialize()
    
    ingestor = get_pdf_ingestor()
    detector = VendorDetector()
    
    # Buscar PDFs
    pdf_files = fm.get_input_pdfs()
    
    if pdf_files:
        print(f"PDFs encontrados: {len(pdf_files)}")
        print(f"Proveedores soportados: {[v.value for v in VendorType if v != VendorType.UNKNOWN]}")
        
        results = []
        for pdf_file in pdf_files[:5]:  # Probar máximo 5 PDFs
            print(f"\n--- Analizando: {pdf_file.name} ---")
            
            # Ingestar PDF
            metadata = ingestor.ingest_single_pdf(pdf_file)
            if metadata.status != PDFStatus.VALID:
                print(f"❌ Error: {metadata.error_message}")
                continue
                
            print(f"✓ PDF válido: {metadata.page_count} páginas, {metadata.file_size // 1024} KB")
            
            # Detectar proveedor
            detection = detector.detect_vendor(metadata)
            results.append(detection)
            
            print(f"🔍 Proveedor detectado: {detection.vendor.value}")
            print(f"📊 Confianza: {detection.confidence:.2f} ({detection.confidence_level.value})")
            print(f"🎯 Firmas coincidentes: {len(detection.matched_signatures)}")
            
            if detection.evidence.get('patterns_found'):
                patterns = detection.evidence['patterns_found']
                print(f"📋 Patrones encontrados: {patterns['total_pattern_matches']}")
                
            # Mostrar scores de todos los proveedores
            if 'vendor_scores' in detection.evidence:
                print("📈 Scores por proveedor:")
                for vendor, score in detection.evidence['vendor_scores'].items():
                    print(f"   {vendor}: {score:.3f}")
        
        # Estadísticas finales
        if results:
            print(f"\n=== Resumen de detecciones ===")
            vendor_counts = Counter(r.vendor.value for r in results)
            avg_confidence = statistics.mean(r.confidence for r in results)
            
            print(f"Total procesados: {len(results)}")
            print(f"Confianza promedio: {avg_confidence:.3f}")
            print("Distribución de proveedores:")
            for vendor, count in vendor_counts.items():
                print(f"  {vendor}: {count}")
                
            # Mostrar estadísticas del detector
            stats = detector.get_detection_stats()
            print(f"\nCache de detección: {stats['cache_size']} entradas")
            
    else:
        print("No se encontraron archivos PDF en input/pdfs/")
        print("Coloca algunos PDFs de catálogos para probar la detección")
        
        # Mostrar firmas cargadas
        signatures = detector.signature_manager.get_all_signatures()
        print(f"\nFirmas de detección cargadas:")
        for vendor, sigs in signatures.items():
            print(f"  {vendor.value}: {len(sigs)} firmas")
    
    print("\nPrueba de detección completada.")