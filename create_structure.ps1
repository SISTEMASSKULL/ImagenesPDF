<#
.SYNOPSIS
Crea la estructura completa de ImagenesPDF con todos los archivos y directorios especificados

.DESCRIPTION
Genera:
1. Estructura completa de archivos y directorios
2. Archivo tree.txt con descripciones
3. Control de sobrescritura selectiva
4. Compatible con todas versiones de PowerShell

.PARAMETER Overwrite
Si se especifica, sobrescribe archivos existentes

.PARAMETER RootPath
Ruta donde crear el proyecto (por defecto: directorio actual)

.EXAMPLE
.\Create-ImagenesPDF.ps1
.\Create-ImagenesPDF.ps1 -Overwrite
.\Create-ImagenesPDF.ps1 -RootPath "C:\Projects"
#>
[CmdletBinding()]
param(
    [switch]$Overwrite = $false,
    [string]$RootPath = (Get-Location).Path
)

# Configuracion
$rootDir = Join-Path $RootPath "ImagenesPDF"
$treeFile = Join-Path $rootDir "tree.txt"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$ErrorActionPreference = 'Stop'

# Mapeo completo de descripciones
$descriptionMap = @{
    # Archivos raiz
    "README.md" = "Documentacion principal del proyecto"
    "pyproject.toml" = "Configuracion de proyecto Python (opcional)"
    "requirements.txt" = "Dependencias de Python"
    "imagenespdf.spec" = "Receta PyInstaller (se genera en build)"
    "run.bat" = "Ejecutador principal del programa"
    "install_runtime.ps1" = "Instalador de dependencias (Python, VC++ redist, Tesseract)"
    "build_exe.bat" = "Script para empaquetar el .exe"

    # src/imagenespdf
    "src/imagenespdf/__init__.py" = "Archivo de inicializacion del paquete"
    "src/imagenespdf/cli.py" = "CLI principal (Typer) con autodeteccion de proveedor"
    "src/imagenespdf/vendor_detector.py" = "Detecta proveedor por CONTENIDO del PDF"
    "src/imagenespdf/code_parser.py" = "Orquesta el parseo de codigos a tokens normalizados"
    "src/imagenespdf/config.py" = "Carga de YAML/JSON de mapeos"
    "src/imagenespdf/logging_setup.py" = "Logging estructurado"
    "src/imagenespdf/utils_fs.py" = "Utilidades de archivos y hashing"
    "src/imagenespdf/years.py" = "Regla de siglos + expansion de años"
    "src/imagenespdf/ingest.py" = "Lectura de PDFs, metadatos"
    "src/imagenespdf/indexer.py" = "Parseo del indice (maker/model/years/page)"
    "src/imagenespdf/detector.py" = "Deteccion de bloques por pagina (si aplica)"
    "src/imagenespdf/color_classifier.py" = "Clasificacion rosa/azul del texto del codigo"
    "src/imagenespdf/image_crop.py" = "Recorte a PNG 1200 DPI (jerarquico + plano)"
    "src/imagenespdf/normalizer.py" = "Normalizacion a dim_* (makers, product_types, etc.)"
    "src/imagenespdf/validators.py" = "Validaciones 3FN y de negocio"
    "src/imagenespdf/writer_excel.py" = "Escritor del libro Excel maestro (unificado)"
    "src/imagenespdf/writer_csv.py" = "CSV espejo por hoja (unificado)"
    "src/imagenespdf/versioning.py" = "Diff + historico"
    "src/imagenespdf/compat.py" = "Resolucion de compatibilidades"

    # codecs
    "src/imagenespdf/codecs/__init__.py" = "Archivo de inicializacion del paquete codecs"
    "src/imagenespdf/codecs/depo_codec.py" = "Decodificador DEPO (A-J + addenda)"
    "src/imagenespdf/codecs/yuto_codec.py" = "Decodificador YUTO (sufijos y tokens)"
    "src/imagenespdf/codecs/hushan_codec.py" = "Decodificador HUSHAN (variantes)"
    "src/imagenespdf/codecs/generic_codec.py" = "Fallback generico"

    # adapters
    "src/imagenespdf/adapters/__init__.py" = "Archivo de inicializacion del paquete adapters"
    "src/imagenespdf/adapters/base.py" = "Interfaz comun para adaptadores"
    "src/imagenespdf/adapters/depo.py" = "Adaptador DEPO (usa depo_codec)"
    "src/imagenespdf/adapters/yuto.py" = "Adaptador YUTO (usa yuto_codec)"
    "src/imagenespdf/adapters/hushan.py" = "Adaptador HUSHAN (usa hushan_codec)"
    "src/imagenespdf/adapters/generic.py" = "Adaptador generico (usa generic_codec)"

    # schema
    "src/imagenespdf/schema/excel_layout.yaml" = "Definicion de hojas/columnas/validaciones"
    "src/imagenespdf/schema/dims.yaml" = "Catalogos iniciales (makers, bulbs, etc.)"
    "src/imagenespdf/schema/features.yaml" = "Taxonomia de features (LED, sensor, keyhole...)"
    "src/imagenespdf/schema/vendor_signatures.yaml" = "Firmas por proveedor para detector"

    # Directorios
    "vendor" = "Dependencias externas embebidas"
    "vendor/tesseract" = "Motor OCR Tesseract portable"
    "vendor/python-embed" = "Python embebido para distribucion"
    "dist" = "Archivos de distribucion (.exe, .msi)"
    "build" = "Archivos temporales de construccion"
    "input" = "Directorio de entrada"
    "input/pdfs" = "PDFs a procesar"
    "out" = "Directorio de salida"
    "out/xlsx" = "Archivos Excel generados"
    "out/csv" = "Archivos CSV espejo"
    "out/images" = "Imagenes extraidas organizadas"
    "out/images/_flat" = "Todas las imagenes en estructura plana"
    "out/logs" = "Archivos de log del procesamiento"
    "src" = "Codigo fuente del proyecto"
    "src/imagenespdf" = "Paquete principal de la aplicacion"
    "src/imagenespdf/codecs" = "Decodificadores especificos por proveedor"
    "src/imagenespdf/adapters" = "Adaptadores de interfaz por proveedor"
    "src/imagenespdf/schema" = "Esquemas y configuraciones YAML"
}

# Configuracion de estructura de archivos
$projectStructure = @{
    "root" = @("README.md", "pyproject.toml", "requirements.txt", "imagenespdf.spec", 
               "run.bat", "install_runtime.ps1", "build_exe.bat")
    
    "src/imagenespdf" = @("__init__.py", "cli.py", "vendor_detector.py", "code_parser.py", 
                          "config.py", "logging_setup.py", "utils_fs.py", "years.py", 
                          "ingest.py", "indexer.py", "detector.py", "color_classifier.py", 
                          "image_crop.py", "normalizer.py", "validators.py", "writer_excel.py", 
                          "writer_csv.py", "versioning.py", "compat.py")
    
    "src/imagenespdf/codecs" = @("__init__.py", "depo_codec.py", "yuto_codec.py", 
                                 "hushan_codec.py", "generic_codec.py")
    
    "src/imagenespdf/adapters" = @("__init__.py", "base.py", "depo.py", "yuto.py", 
                                   "hushan.py", "generic.py")
    
    "src/imagenespdf/schema" = @("excel_layout.yaml", "dims.yaml", "features.yaml", 
                                 "vendor_signatures.yaml")
}

# Directorios a crear (sin archivos)
$directories = @(
    "vendor/tesseract",
    "vendor/python-embed", 
    "dist",
    "build",
    "input/pdfs",
    "out/xlsx",
    "out/csv", 
    "out/images/_flat",
    "out/logs"
)

# Funcion de logging mejorada
function Write-LogMessage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Message,
        
        [ValidateSet('Info', 'Success', 'Warning', 'Error')]
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

# Funcion para crear directorios con validacion mejorada
function New-SafeDirectory {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Path)
    
    try {
        if (Test-Path -Path $Path -PathType Container) {
            Write-LogMessage "Directorio ya existe: $Path" -Level 'Warning'
            return $true
        }
        
        $null = New-Item -ItemType Directory -Path $Path -Force
        Write-LogMessage "Directorio creado: $Path" -Level 'Success'
        return $true
        
    } catch {
        Write-LogMessage "Error creando directorio '$Path': $($_.Exception.Message)" -Level 'Error'
        return $false
    }
}

# Funcion para crear archivos con validacion mejorada
function New-SafeFile {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$FilePath)
    
    try {
        $fileExists = Test-Path -Path $FilePath -PathType Leaf
        
        if ($fileExists -and -not $Overwrite) {
            Write-LogMessage "Archivo ya existe (omitiendo): $FilePath" -Level 'Warning'
            return $true
        }
        
        # Verificar que el directorio padre existe
        $parentDir = Split-Path -Parent $FilePath
        if ($parentDir -and -not (Test-Path -Path $parentDir)) {
            Write-LogMessage "Directorio padre no existe para: $FilePath" -Level 'Error'
            return $false
        }
        
        $null = New-Item -ItemType File -Path $FilePath -Force
        
        $action = if ($fileExists) { "sobrescrito" } else { "creado" }
        Write-LogMessage "Archivo ${action}: $FilePath" -Level 'Success'
        return $true
        
    } catch {
        Write-LogMessage "Error creando archivo '$FilePath': $($_.Exception.Message)" -Level 'Error'
        return $false
    }
}

# Funcion para generar tree.txt con caracteres ASCII seguros
function New-TreeFile {
    [CmdletBinding()]
    param()
    
    try {
        $script:treeContent = @"
Estructura del proyecto ImagenesPDF
==================================
Generado: $timestamp
Modo sobrescritura: $Overwrite
Ruta base: $rootDir

ImagenesPDF/
"@

        function Get-DirectoryTree {
            param(
                [string]$FolderPath, 
                [string]$Indent = ""
            )
            
            if (-not (Test-Path -Path $FolderPath)) { return }
            
            $items = Get-ChildItem $FolderPath -Force | Sort-Object @{Expression={$_.PSIsContainer}; Descending=$true}, Name
            $itemCount = $items.Count
            
            for ($i = 0; $i -lt $itemCount; $i++) {
                $item = $items[$i]
                $isLastItem = ($i -eq ($itemCount - 1))
                
                # Usar caracteres ASCII seguros
                $connector = if ($isLastItem) { "+-- " } else { "|-- " }
                $line = $Indent + $connector + $item.Name
                
                # Buscar descripcion
                $relativePath = $item.FullName.Substring($rootDir.Length + 1).Replace("\", "/")
                
                # Primero buscar por ruta relativa, luego por nombre de archivo
                $description = $null
                if ($descriptionMap.ContainsKey($relativePath)) {
                    $description = $descriptionMap[$relativePath]
                } elseif ($descriptionMap.ContainsKey($item.Name)) {
                    $description = $descriptionMap[$item.Name]
                }
                
                if ($description) {
                    $line += " - " + $description
                }
                
                $script:treeContent += $line + "`n"
                
                # Si es un directorio, procesarlo recursivamente
                if ($item.PSIsContainer) {
                    $newIndent = if ($isLastItem) { 
                        $Indent + "    " 
                    } else { 
                        $Indent + "|   " 
                    }
                    Get-DirectoryTree -FolderPath $item.FullName -Indent $newIndent
                }
            }
        }

        # Ejecutar la funcion para generar el arbol
        Get-DirectoryTree -FolderPath $rootDir
        
        # Agregar estadisticas al final
        $fileCount = (Get-ChildItem $rootDir -Recurse -File).Count
        $dirCount = (Get-ChildItem $rootDir -Recurse -Directory).Count
        
        $script:treeContent += "`n"
        $script:treeContent += "=== ESTADISTICAS ===`n"
        $script:treeContent += "- Directorios: $dirCount`n"
        $script:treeContent += "- Archivos: $fileCount`n"
        $script:treeContent += "`n"
        $script:treeContent += "=== DESCRIPCION GENERAL ===`n"
        $script:treeContent += "Este proyecto procesa catalogos PDF de autopartes, extrayendo codigos,`n"
        $script:treeContent += "imagenes y metadatos para generar archivos Excel y CSV normalizados.`n"
        $script:treeContent += "Soporta multiples proveedores (DEPO, YUTO, HUSHAN) con deteccion automatica.`n"
        
        # Escribir el contenido al archivo
        $script:treeContent | Out-File -FilePath $treeFile -Force -Encoding UTF8
        Write-LogMessage "Archivo tree.txt generado correctamente con $fileCount archivos y $dirCount directorios" -Level 'Success'
        return $true
        
    } catch {
        Write-LogMessage "Error generando tree.txt: $($_.Exception.Message)" -Level 'Error'
        return $false
    }
}

# Funcion principal
function Initialize-ProjectStructure {
    [CmdletBinding()]
    param()
    
    Write-LogMessage "Iniciando creacion de estructura en: $rootDir" -Level 'Info'
    
    $successCount = 0
    $errorCount = 0
    
    try {
        # 1. Crear directorio raiz
        if (-not (New-SafeDirectory -Path $rootDir)) {
            throw "No se pudo crear el directorio raiz"
        }
        
        # 2. Crear estructura de archivos
        foreach ($pathKey in $projectStructure.Keys) {
            $targetPath = if ($pathKey -eq "root") { $rootDir } else { Join-Path $rootDir $pathKey }
            
            # Crear directorio si no es root
            if ($pathKey -ne "root") {
                if (-not (New-SafeDirectory -Path $targetPath)) {
                    $errorCount++
                    continue
                }
            }
            
            # Crear archivos
            foreach ($fileName in $projectStructure[$pathKey]) {
                $filePath = Join-Path $targetPath $fileName
                if (New-SafeFile -FilePath $filePath) {
                    $successCount++
                } else {
                    $errorCount++
                }
            }
        }
        
        # 3. Crear directorios vacios
        foreach ($dir in $directories) {
            $dirPath = Join-Path $rootDir $dir
            if (New-SafeDirectory -Path $dirPath) {
                $successCount++
            } else {
                $errorCount++
            }
        }
        
        # 4. Generar tree.txt
        if (New-TreeFile) {
            $successCount++
        } else {
            $errorCount++
        }
        
        # 5. Resumen final
        Write-Host "`n" -NoNewline
        Write-LogMessage "=== RESUMEN DE EJECUCION ===" -Level 'Info'
        Write-LogMessage "Operaciones exitosas: $successCount" -Level 'Success'
        
        if ($errorCount -gt 0) {
            Write-LogMessage "Operaciones con error: $errorCount" -Level 'Error'
            Write-LogMessage "Revise los mensajes anteriores para mas detalles" -Level 'Warning'
        } else {
            Write-LogMessage "Estructura creada exitosamente sin errores!" -Level 'Success'
        }
        
        Write-LogMessage "Detalles completos en: $treeFile" -Level 'Info'
        
    } catch {
        Write-LogMessage "Error critico: $($_.Exception.Message)" -Level 'Error'
        exit 1
    }
}

# Ejecucion principal
if ($MyInvocation.InvocationName -ne '.') {
    Initialize-ProjectStructure
}