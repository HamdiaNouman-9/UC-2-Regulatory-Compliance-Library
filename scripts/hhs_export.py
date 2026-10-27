"""Export HHS -- with a PDF-reader fallback for when Windows blocks PyMuPDF.

    python scripts/hhs_export.py --out output/workbooks/hhs_noFR.xlsx

Same as `python -m tools.workbook export hhs ...` (any further arguments are
passed through), with one difference.

MEASURED 2026-10-16: after a Windows update (KB5121794, KB5127648, KB5127694)
Smart App Control began refusing PyMuPDF's unsigned engine on this machine
("DLL load failed while importing _mupdf: An Application Control policy has
blocked this file"). The orchestrator imports `fitz` at load time, so every
export died before crawling. pypdfium2 (Google's PDF engine, signed) still
loads. When -- and only when -- PyMuPDF cannot be imported, this script puts a
stand-in `fitz` built on pypdfium2 in its place for THIS run.

HHS ONLY, by the user's choice (2026-10-16). Other regulators keep the normal
command and PyMuPDF. Once Smart App Control is off or PyMuPDF is allowed, the
real `fitz` imports and this script behaves exactly like the normal command.

CAVEAT: pypdfium2's text differs slightly from PyMuPDF's (line breaks,
spacing). A workbook built on the stand-in is a stopgap; do not make it the
baseline that a later PyMuPDF export is compared against, or many PDFs read as
changed when they are not.

The stand-in covers only what the export path calls (processor/
Text_Extractor.py): fitz.open (also as a context manager), len(doc), doc[i],
doc.close(), page.get_text("text"), page.get_pixmap(matrix=fitz.Matrix(s, s))
-> .width .height .samples (RGB bytes). Anything else raises.
"""
import os
import sys
import types

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
os.chdir(REPO)


def _pdfium_fitz() -> types.ModuleType:
    import pypdfium2 as pdfium

    class Matrix:
        def __init__(self, a, d=None):
            self.a, self.d = a, (a if d is None else d)

    class _Pix:
        def __init__(self, img):
            img = img.convert("RGB")
            self.width, self.height = img.size
            self.samples = img.tobytes()

    class _Page:
        def __init__(self, page):
            self._p = page

        def get_text(self, kind="text", **_):
            if kind != "text":
                raise NotImplementedError("pypdfium2 stand-in: get_text(%r)" % kind)
            tp = self._p.get_textpage()
            try:
                return tp.get_text_range().replace("\r\n", "\n")
            finally:
                tp.close()

        def get_pixmap(self, matrix=None, **_):
            return _Pix(self._p.render(scale=matrix.a if matrix else 1).to_pil())

    class _Doc:
        def __init__(self, path):
            self._d = pdfium.PdfDocument(path)

        def __len__(self):
            return len(self._d)

        def __getitem__(self, i):
            return _Page(self._d[i])

        def __iter__(self):
            return (self[i] for i in range(len(self)))

        def close(self):
            self._d.close()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.close()

    m = types.ModuleType("fitz")
    m.open = _Doc
    m.Matrix = Matrix
    m.__doc__ = "pypdfium2 stand-in for PyMuPDF (scripts/hhs_export.py)"
    return m


def main() -> int:
    if any(a in ("--form", "--source") or a.startswith("--source=")
           for a in sys.argv[1:]):
        raise SystemExit("hhs_export.py runs the HHS export only.")
    try:
        import fitz  # noqa: F401
        print("PDF reader: PyMuPDF")
    except ImportError as e:
        for k in [k for k in sys.modules if k == "fitz" or k.startswith("pymupdf")]:
            del sys.modules[k]
        sys.modules["fitz"] = _pdfium_fitz()
        print("PDF reader: pypdfium2 stand-in -- PyMuPDF would not load (%s)"
              % str(e).splitlines()[0][:120])
    from tools.workbook import main as workbook_main
    sys.argv = ["tools.workbook", "export", "hhs"] + sys.argv[1:]
    return workbook_main()


if __name__ == "__main__":
    raise SystemExit(main())
