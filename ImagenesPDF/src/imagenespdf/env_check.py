import importlib, sys, platform

mods = [
    "fitz", "pdfplumber", "pypdfium2", "PIL", "cv2",
    "pandas", "openpyxl", "xlsxwriter", "typer", "rich", "yaml", "structlog",
    "pytesseract"
]
print("=== ImagenesPDF Environment Report ===")
print("Python:", sys.version.replace("\n"," "))
print("Platform:", platform.platform())
ok = True
for m in mods:
    try:
        importlib.import_module(m)
        print(f"[OK] {m}")
    except Exception as e:
        ok = False
        print(f"[FAIL] {m}: {e}")

sys.exit(0 if ok else 1)
