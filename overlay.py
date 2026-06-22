"""Screen-edge glow overlay for computer-use (Windows).

A topmost, click-through, per-pixel-alpha layered window that paints a soft pulsing glow
around the screen edges while Source Agent is driving the desktop — the same kind of
indicator Claude's computer use shows. Pure ctypes + Pillow (already bundled), so it needs
no tkinter and works in frozen PyInstaller builds. Safe no-op on non-Windows or any failure.

Public API:  overlay.show()  /  overlay.hide()   (both idempotent and exception-safe)
"""
import sys
import math
import threading

_GLOW = None
_GLOW_LOCK = threading.Lock()

# Keep the window-class proc alive for the process lifetime (Windows holds a raw pointer to
# it; if Python GCs it while the class is registered, a later window call would crash).
_WNDPROC_REF = None
_CLASS_REGISTERED = False
_CLASS_NAME = "SourceAgentEdgeGlow"


def show():
    """Start (or keep showing) the edge glow. Idempotent and never raises."""
    global _GLOW
    if sys.platform != "win32":
        return
    with _GLOW_LOCK:
        try:
            if _GLOW is None:
                _GLOW = _EdgeGlow()
                _GLOW.start()
            _GLOW.visible = True
        except Exception as e:
            print(f"overlay show failed: {e}")


def hide():
    """Stop the edge glow. Idempotent and never raises."""
    with _GLOW_LOCK:
        try:
            if _GLOW is not None:
                _GLOW.visible = False
        except Exception as e:
            print(f"overlay hide failed: {e}")


class _EdgeGlow:
    def __init__(self, color=(150, 90, 255), thickness=120):
        self.color = color          # RGB of the glow
        self.thickness = thickness  # glow band width in px
        self._stop = threading.Event()
        self.visible = False
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    # --- build the premultiplied BGRA glow bitmap with Pillow (all C-level, fast) ---
    def _build_bitmap(self, sw, sh):
        from PIL import Image, ImageDraw, ImageFilter, ImageChops
        r, g, b = self.color
        img = Image.new("RGBA", (sw, sh), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        d.rectangle([0, 0, sw - 1, sh - 1], outline=(r, g, b, 255), width=max(3, self.thickness // 2))
        img = img.filter(ImageFilter.GaussianBlur(self.thickness // 3))
        # crisp bright inner edge on top of the soft halo
        ImageDraw.Draw(img).rectangle(
            [1, 1, sw - 2, sh - 2],
            outline=(min(r + 70, 255), min(g + 70, 255), min(b + 70, 255), 235), width=3)
        R, G, B, A = img.split()
        # premultiply by alpha and reorder to B,G,R,A for UpdateLayeredWindow
        prem = Image.merge("RGBA", (ImageChops.multiply(B, A), ImageChops.multiply(G, A),
                                    ImageChops.multiply(R, A), A))
        return prem.tobytes("raw", "RGBA")  # bytes land as B,G,R,A premultiplied

    def _run(self):
        global _WNDPROC_REF, _CLASS_REGISTERED
        try:
            import ctypes
            from ctypes import wintypes
        except Exception:
            return

        try:
            import pyautogui
            sw, sh = pyautogui.size()
        except Exception:
            u = ctypes.windll.user32
            sw, sh = u.GetSystemMetrics(0), u.GetSystemMetrics(1)

        user32 = ctypes.windll.user32
        gdi32 = ctypes.windll.gdi32
        kernel32 = ctypes.windll.kernel32

        # argtypes/restypes — required so 64-bit handles aren't truncated to int
        user32.CreateWindowExW.restype = wintypes.HWND
        user32.CreateWindowExW.argtypes = [wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD,
                                           ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                           wintypes.HWND, wintypes.HMENU, wintypes.HINSTANCE, wintypes.LPVOID]
        user32.GetDC.restype = wintypes.HDC; user32.GetDC.argtypes = [wintypes.HWND]
        user32.ReleaseDC.argtypes = [wintypes.HWND, wintypes.HDC]
        user32.DefWindowProcW.restype = ctypes.c_long
        user32.DefWindowProcW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
        user32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
        user32.DestroyWindow.argtypes = [wintypes.HWND]
        user32.SetWindowPos.argtypes = [wintypes.HWND, wintypes.HWND, ctypes.c_int, ctypes.c_int,
                                        ctypes.c_int, ctypes.c_int, wintypes.UINT]
        user32.UpdateLayeredWindow.argtypes = [wintypes.HWND, wintypes.HDC, ctypes.c_void_p, ctypes.c_void_p,
                                               wintypes.HDC, ctypes.c_void_p, wintypes.COLORREF,
                                               ctypes.c_void_p, wintypes.DWORD]
        user32.PeekMessageW.argtypes = [ctypes.c_void_p, wintypes.HWND, wintypes.UINT, wintypes.UINT, wintypes.UINT]
        user32.PeekMessageW.restype = wintypes.BOOL
        user32.TranslateMessage.argtypes = [ctypes.c_void_p]
        user32.TranslateMessage.restype = wintypes.BOOL
        user32.DispatchMessageW.argtypes = [ctypes.c_void_p]
        user32.DispatchMessageW.restype = ctypes.c_long

        gdi32.CreateCompatibleDC.restype = wintypes.HDC; gdi32.CreateCompatibleDC.argtypes = [wintypes.HDC]
        gdi32.CreateDIBSection.restype = wintypes.HBITMAP
        gdi32.CreateDIBSection.argtypes = [wintypes.HDC, ctypes.c_void_p, wintypes.UINT,
                                           ctypes.POINTER(ctypes.c_void_p), wintypes.HANDLE, wintypes.DWORD]
        gdi32.SelectObject.restype = wintypes.HGDIOBJ; gdi32.SelectObject.argtypes = [wintypes.HDC, wintypes.HGDIOBJ]
        gdi32.DeleteObject.argtypes = [wintypes.HGDIOBJ]
        gdi32.DeleteDC.argtypes = [wintypes.HDC]
        kernel32.GetModuleHandleW.restype = wintypes.HMODULE

        WS_EX = 0x00080000 | 0x00000020 | 0x00000008 | 0x00000080 | 0x08000000  # LAYERED|TRANSPARENT|TOPMOST|TOOLWINDOW|NOACTIVATE
        WS_POPUP = 0x80000000
        hInst = kernel32.GetModuleHandleW(None)

        # register the window class once for the process; keep the proc reference alive
        WNDPROC = ctypes.WINFUNCTYPE(ctypes.c_long, wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM)

        class WNDCLASS(ctypes.Structure):
            _fields_ = [("style", wintypes.UINT), ("lpfnWndProc", WNDPROC), ("cbClsExtra", ctypes.c_int),
                        ("cbWndExtra", ctypes.c_int), ("hInstance", wintypes.HINSTANCE), ("hIcon", wintypes.HICON),
                        ("hCursor", wintypes.HANDLE), ("hbrBackground", wintypes.HBRUSH),
                        ("lpszMenuName", wintypes.LPCWSTR), ("lpszClassName", wintypes.LPCWSTR)]

        if not _CLASS_REGISTERED:
            _WNDPROC_REF = WNDPROC(lambda h, m, w, l: user32.DefWindowProcW(h, m, w, l))
            cls = WNDCLASS()
            cls.lpfnWndProc = _WNDPROC_REF
            cls.hInstance = hInst
            cls.lpszClassName = _CLASS_NAME
            user32.RegisterClassW(ctypes.byref(cls))
            _CLASS_REGISTERED = True

        hwnd = user32.CreateWindowExW(WS_EX, _CLASS_NAME, "EdgeGlow", WS_POPUP, 0, 0, sw, sh,
                                      None, None, hInst, None)
        if not hwnd:
            return

        # 32bpp top-down DIB section we can write pixels into
        class BMIH(ctypes.Structure):
            _fields_ = [("biSize", wintypes.DWORD), ("biWidth", wintypes.LONG), ("biHeight", wintypes.LONG),
                        ("biPlanes", wintypes.WORD), ("biBitCount", wintypes.WORD), ("biCompression", wintypes.DWORD),
                        ("biSizeImage", wintypes.DWORD), ("biXPPM", wintypes.LONG), ("biYPPM", wintypes.LONG),
                        ("biClrUsed", wintypes.DWORD), ("biClrImp", wintypes.DWORD)]
        bmi = BMIH(); bmi.biSize = ctypes.sizeof(BMIH); bmi.biWidth = sw; bmi.biHeight = -sh
        bmi.biPlanes = 1; bmi.biBitCount = 32; bmi.biCompression = 0

        screenDC = user32.GetDC(None)
        memDC = gdi32.CreateCompatibleDC(screenDC)
        bits = ctypes.c_void_p()
        hbmp = gdi32.CreateDIBSection(screenDC, ctypes.byref(bmi), 0, ctypes.byref(bits), None, 0)
        gdi32.SelectObject(memDC, hbmp)

        try:
            raw = self._build_bitmap(sw, sh)
            ctypes.memmove(bits, raw, len(raw))
        except Exception as e:
            print(f"overlay bitmap failed: {e}")
            user32.DestroyWindow(hwnd)
            return

        class POINT(ctypes.Structure):
            _fields_ = [("x", wintypes.LONG), ("y", wintypes.LONG)]

        class SIZE(ctypes.Structure):
            _fields_ = [("cx", wintypes.LONG), ("cy", wintypes.LONG)]

        class BLEND(ctypes.Structure):
            _fields_ = [("Op", ctypes.c_ubyte), ("Flags", ctypes.c_ubyte),
                        ("Alpha", ctypes.c_ubyte), ("Fmt", ctypes.c_ubyte)]

        class MSG(ctypes.Structure):
            _fields_ = [("hwnd", wintypes.HWND),
                        ("message", wintypes.UINT),
                        ("wParam", wintypes.WPARAM),
                        ("lParam", wintypes.LPARAM),
                        ("time", wintypes.DWORD),
                        ("pt", POINT)]

        ptDst, ptSrc, size = POINT(0, 0), POINT(0, 0), SIZE(sw, sh)
        ULW_ALPHA = 0x02
        HWND_TOPMOST = ctypes.c_void_p(-1)
        SWP = 0x0010 | 0x0002 | 0x0001  # NOACTIVATE | NOMOVE | NOSIZE

        i = 0
        last_visible = False
        while not self._stop.is_set():
            # Pump messages for this window to prevent "Not Responding" state
            msg = MSG()
            while user32.PeekMessageW(ctypes.byref(msg), None, 0, 0, 1): # PM_REMOVE = 1
                user32.TranslateMessage(ctypes.byref(msg))
                user32.DispatchMessageW(ctypes.byref(msg))

            cur_visible = self.visible
            if cur_visible:
                if not last_visible:
                    user32.ShowWindow(hwnd, 8)      # SW_SHOWNA
                    last_visible = True
                
                a = int(150 + 90 * (0.5 + 0.5 * math.sin(i / 9.0)))   # gentle breathing 60..240
                blend = BLEND(0, 0, max(0, min(255, a)), 0x01)        # AC_SRC_OVER, AC_SRC_ALPHA
                user32.UpdateLayeredWindow(hwnd, screenDC, ctypes.byref(ptDst), ctypes.byref(size),
                                           memDC, ctypes.byref(ptSrc), 0, ctypes.byref(blend), ULW_ALPHA)
                user32.SetWindowPos(hwnd, HWND_TOPMOST, 0, 0, 0, 0, SWP)
                i += 1
            else:
                if last_visible:
                    user32.ShowWindow(hwnd, 0)      # SW_HIDE
                    last_visible = False

            self._stop.wait(0.04)

        try:
            user32.DestroyWindow(hwnd)
            gdi32.DeleteObject(hbmp)
            gdi32.DeleteDC(memDC)
            user32.ReleaseDC(None, screenDC)
        except Exception:
            pass
