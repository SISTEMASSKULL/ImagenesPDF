# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['D:\\ImagenesPDF\\ImagenesPDF\\src\\imagenespdf\\cli.py'],
    pathex=[],
    binaries=[],
    datas=[('src\\imagenespdf\\schema\\excel_layout.yaml', 'imagenespdf\\schema'), ('src\\imagenespdf\\schema\\dims.yaml', 'imagenespdf\\schema'), ('src\\imagenespdf\\schema\\features.yaml', 'imagenespdf\\schema')],
    hiddenimports=['fitz', 'cv2'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='extractor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
