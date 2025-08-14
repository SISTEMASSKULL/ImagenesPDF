<#
.SYNOPSIS
Genera un archivo memory.txt con el contenido de todos los archivos relevantes para el desarrollo Python

.DESCRIPTION
Extrae el contenido de archivos Python, YAML, configuración y documentación,
excluyendo archivos binarios, ejecutables y scripts de sistema no relevantes
para el seguimiento del progreso de desarrollo.

.PARAMETER RootPath
Ruta del proyecto ImagenesPDF (por defecto: directorio actual/ImagenesPDF)

.PARAMETER OutputFile
Nombre del archivo de salida (por defecto: memory.txt)

.EXAMPLE
.\Generate-Memory.ps1
.\Generate-Memory.ps1 -RootPath "C:\Projects\ImagenesPDF"
.\Generate-Memory.ps1 -OutputFile "development_snapshot.txt"
#>
[CmdletBinding()]
param(
    [string]$RootPath = (Join-Path (Get-Location).Path "ImagenesPDF"),
    [string]$OutputFile = "memory.txt"
)

# Configuracion
$memoryFile = Join-Path $RootPath $OutputFile
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$ErrorActionPreference = 'Continue'

# Archivos a incluir (extensiones y nombres especificos)
$includeExtensions = @('.py', '.yaml', '.yml', '.md', '.txt', '.toml', '.spec', '.ps1')
$excludeFiles = @('run.bat', 'build_exe.bat', 'install_runtime.ps1', 'tree.txt', 'memory.txt')
$excludeDirectories = @('dist', 'build', '__pycache__', '.git', 'vendor', '.venv')

# Descripciones de proposito por archivo
$fileDescriptions = @{
    "__init__.py" = "Inicializacion del paquete Python"
    "cli.py" = "Interfaz de linea de comandos principal"
    "vendor_detector.py" = "Deteccion automatica de proveedor por contenido PDF"
    "code_parser.py" = "Orquestador de parseo de codigos"
    "config.py" = "Carga y gestion de configuraciones"
    "logging_setup.py" = "Configuracion del sistema de logging"
    "utils_fs.py" = "Utilidades del sistema de archivos"
    "years.py" = "Logica de manejo y expansion de años"
    "ingest.py" = "Ingesta y lectura de archivos PDF"
    "indexer.py" = "Parseo e indexacion de contenido"
    "detector.py" = "Deteccion de elementos en paginas"
    "color_classifier.py" = "Clasificacion de colores de texto"
    "image_crop.py" = "Recorte y procesamiento de imagenes"
    "normalizer.py" = "Normalizacion de datos"
    "validators.py" = "Validaciones de negocio y integridad"
    "writer_excel.py" = "Generacion de archivos Excel"
    "writer_csv.py" = "Generacion de archivos CSV"
    "versioning.py" = "Control de versiones y diferencias"
    "compat.py" = "Gestion de compatibilidades"
    "depo_codec.py" = "Codec especifico para proveedor DEPO"
    "yuto_codec.py" = "Codec especifico para proveedor YUTO"
    "hushan_codec.py" = "Codec especifico para proveedor HUSHAN"
    "generic_codec.py" = "Codec generico fallback"
    "base.py" = "Interfaz base para adaptadores"
    "depo.py" = "Adaptador para proveedor DEPO"
    "yuto.py" = "Adaptador para proveedor YUTO"
    "hushan.py" = "Adaptador para proveedor HUSHAN"
    "generic.py" = "Adaptador generico"
    "excel_layout.yaml" = "Definicion de estructura de Excel"
    "dims.yaml" = "Catalogos y dimensiones de datos"
    "features.yaml" = "Taxonomia de caracteristicas"
    "vendor_signatures.yaml" = "Firmas de identificacion de proveedores"
    "README.md" = "Documentacion principal del proyecto"
    "requirements.txt" = "Dependencias Python del proyecto"
    "pyproject.toml" = "Configuracion del proyecto Python"
    "imagenespdf.spec" = "Especificacion para PyInstaller"
}

# Funcion de logging
function Write-LogMessage {
    param(
        [string]$Message,
        [string]$Level = 'Info'
    )
    
    $colors = @{
        'Info' = 'White'
        'Success' = 'Green'
        'Warning' = 'Yellow'
        'Error' = 'Red'
    }
    
    $prefix = switch ($Level) {
        'Success' { '[OK]' }
        'Warning' { '[WARN]' }
        'Error' { '[ERROR]' }
        default { '[INFO]' }
    }
    
    Write-Host "[$((Get-Date).ToString('HH:mm:ss'))] $prefix $Message" -ForegroundColor $colors[$Level]
}

# Funcion para verificar si un archivo debe incluirse
function Should-IncludeFile {
    param(
        [System.IO.FileInfo]$File,
        [string]$RelativePath
    )
    
    # Verificar extension
    if ($includeExtensions -notcontains $File.Extension.ToLower()) {
        return $false
    }
    
    # Verificar archivos excluidos
    if ($excludeFiles -contains $File.Name) {
        return $false
    }
    
    # Verificar directorios excluidos
    foreach ($excludeDir in $excludeDirectories) {
        if ($RelativePath -like "*$excludeDir*") {
            return $false
        }
    }
    
    return $true
}

# Funcion para generar separador visual
function Get-FileSeparator {
    param([string]$FilePath, [string]$Description = "")
    
    $separator = "=" * 80
    $header = "ARCHIVO: $FilePath"
    if ($Description) {
        $header += "`nPROPOSITO: $Description"
    }
    $header += "`nTAMAÑO: $((Get-Item $FilePath -ErrorAction SilentlyContinue).Length) bytes"
    
    return "`n$separator`n$header`n$separator`n"
}

# Funcion principal para generar memory.txt
function New-MemoryFile {
    try {
        if (-not (Test-Path $RootPath)) {
            throw "El directorio del proyecto no existe: $RootPath"
        }
        
        Write-LogMessage "Iniciando generacion de memory.txt en: $RootPath"
        
        # Inicializar contenido
        $memoryContent = @"
MEMORIA DE DESARROLLO - PROYECTO IMAGENESPDF
============================================
Generado: $timestamp
Proyecto: ImagenesPDF - Procesador de catalogos PDF de autopartes
Ruta base: $RootPath

DESCRIPCION:
Este archivo contiene una captura completa del codigo fuente y configuraciones
del proyecto ImagenesPDF. Incluye todos los archivos Python, configuraciones
YAML, documentacion y especificaciones relevantes para el desarrollo.

EXCLUYE:
- Archivos binarios (.exe, .dll, etc.)
- Scripts de sistema (.bat)
- Directorios de construccion (dist/, build/)
- Archivos temporales y cache
- Dependencias externas (vendor/)

ESTRUCTURA DE CONTENIDO:
Cada archivo se presenta con un separador que incluye:
- Ruta relativa del archivo
- Proposito/funcion del archivo
- Tamaño en bytes
- Contenido completo del archivo

============================================

"@

        # Obtener todos los archivos relevantes
        $allFiles = Get-ChildItem -Path $RootPath -Recurse -File | Where-Object {
            $relativePath = $_.FullName.Substring($RootPath.Length + 1)
            Should-IncludeFile -File $_ -RelativePath $relativePath
        }
        
        Write-LogMessage "Encontrados $($allFiles.Count) archivos relevantes para incluir"
        
        $processedCount = 0
        $errorCount = 0
        
        # Procesar archivos ordenados por ruta
        foreach ($file in ($allFiles | Sort-Object FullName)) {
            try {
                $relativePath = $file.FullName.Substring($RootPath.Length + 1).Replace("\", "/")
                $description = $fileDescriptions[$file.Name]
                
                Write-LogMessage "Procesando: $relativePath"
                
                # Agregar separador
                $memoryContent += Get-FileSeparator -FilePath $relativePath -Description $description
                
                # Leer contenido del archivo
                try {
                    $fileContent = Get-Content -Path $file.FullName -Raw -Encoding UTF8
                    if ($null -eq $fileContent -or $fileContent.Trim() -eq "") {
                        $fileContent = "[ARCHIVO VACIO]"
                    }
                } catch {
                    $fileContent = "[ERROR LEYENDO ARCHIVO: $($_.Exception.Message)]"
                    Write-LogMessage "Error leyendo $($file.Name): $($_.Exception.Message)" -Level 'Warning'
                }
                
                $memoryContent += $fileContent
                $memoryContent += "`n`n"
                $processedCount++
                
            } catch {
                Write-LogMessage "Error procesando $($file.FullName): $($_.Exception.Message)" -Level 'Error'
                $errorCount++
            }
        }
        
        # Agregar estadisticas finales
        $memoryContent += @"

============================================
ESTADISTICAS DE GENERACION
============================================
Archivos procesados: $processedCount
Errores encontrados: $errorCount
Tamaño total del memory.txt: $(($memoryContent | Measure-Object -Character).Characters) caracteres
Generado el: $timestamp

TIPOS DE ARCHIVO INCLUIDOS:
$($includeExtensions -join ', ')

ARCHIVOS EXCLUIDOS:
$($excludeFiles -join ', ')

DIRECTORIOS EXCLUIDOS:
$($excludeDirectories -join ', ')

============================================
FIN DE LA MEMORIA DE DESARROLLO
============================================
"@

        # Escribir archivo
        $memoryContent | Out-File -FilePath $memoryFile -Force -Encoding UTF8
        
        $fileSizeMB = [math]::Round((Get-Item $memoryFile).Length / 1MB, 2)
        
        Write-LogMessage "memory.txt generado exitosamente!" -Level 'Success'
        Write-LogMessage "Ubicacion: $memoryFile" -Level 'Success'
        Write-LogMessage "Tamaño: $fileSizeMB MB" -Level 'Success'
        Write-LogMessage "Archivos incluidos: $processedCount" -Level 'Success'
        
        if ($errorCount -gt 0) {
            Write-LogMessage "Se encontraron $errorCount errores durante el procesamiento" -Level 'Warning'
        }
        
    } catch {
        Write-LogMessage "Error critico generando memory.txt: $($_.Exception.Message)" -Level 'Error'
        throw
    }
}

# Verificar parametros y ejecutar
if (-not (Test-Path $RootPath)) {
    Write-LogMessage "El directorio especificado no existe: $RootPath" -Level 'Error'
    Write-LogMessage "Asegurate de que el proyecto ImagenesPDF existe en la ruta especificada" -Level 'Info'
    exit 1
}

# Ejecutar generacion
New-MemoryFile

Write-LogMessage "Proceso completado. Revisa el archivo $OutputFile para ver el contenido completo del proyecto." -Level 'Info'