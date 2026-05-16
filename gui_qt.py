
import os
import sys

if getattr(sys, "frozen", False):
    _bundle_dir = sys._MEIPASS
    os.environ["PATH"] = _bundle_dir + os.pathsep + os.environ.get("PATH", "")
import json
import queue
import random
import shutil
import subprocess
import tempfile
import threading
import time
from datetime import datetime

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QSplitter,
    QVBoxLayout, QHBoxLayout, QGridLayout,
    QListWidget, QListWidgetItem, QPushButton, QLabel,
    QLineEdit, QCheckBox, QDoubleSpinBox, QSpinBox,
    QComboBox, QTabWidget, QScrollArea, QGroupBox,
    QProgressBar, QTextEdit, QFileDialog, QMessageBox,
    QMenu, QFrame, QStackedWidget, QAbstractItemView,
    QSystemTrayIcon, QSizePolicy,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QStandardPaths, QTimer, QSize
from PyQt6.QtGui import (
    QColor, QFont, QAction, QKeySequence, QShortcut,
    QDragEnterEvent, QDropEvent, QPixmap, QImage, QPalette, QIcon,
)

from core_logic import TrackInfo, ModificationWorker, BatchProcessor, BatchConverter


AUDIO_EXTENSIONS = {
    ".mp3", ".flac", ".wav", ".ogg", ".aac", ".m4a", ".wma", ".opus",
    ".aiff", ".alac", ".wv", ".ape", ".tta", ".ac3", ".dts", ".mp2",
    ".mpc", ".spx", ".amr", ".au", ".mka", ".oga", ".caf", ".shn",
}

SUPPORTED_FORMATS = {
    "mp3": "MP3 (lossy)", "flac": "FLAC (lossless)", "wav": "WAV (lossless)",
    "ogg": "OGG Vorbis (lossy)", "aac": "AAC (lossy)", "m4a": "M4A AAC (lossy)",
    "wma": "WMA (lossy)", "opus": "Opus (lossy)", "aiff": "AIFF (lossless)",
    "alac": "ALAC (lossless)", "wv": "WavPack", "ape": "Monkey's Audio",
    "tta": "TrueAudio", "ac3": "AC3/Dolby", "dts": "DTS",
    "mp2": "MPEG Layer 2", "mpc": "Musepack", "spx": "Speex",
    "amr": "AMR", "au": "AU/Sun", "mka": "Matroska",
    "oga": "Ogg FLAC", "caf": "Core Audio", "shn": "Shorten",
}

QUALITY_PRESETS = {
    "mp3": ["320 kbps (CBR)", "256 kbps (CBR)", "192 kbps (CBR)", "128 kbps (CBR)",
            "VBR Высшее (Q0)", "VBR Высокое (Q2)", "VBR Среднее (Q4)", "VBR Низкое (Q6)"],
    "aac": ["320 kbps", "256 kbps", "192 kbps", "128 kbps"],
    "m4a": ["320 kbps", "256 kbps", "192 kbps", "128 kbps"],
    "ogg": ["Качество 10", "Качество 8", "Качество 6", "Качество 4", "Качество 2"],
    "opus": ["256 kbps", "192 kbps", "128 kbps", "96 kbps", "64 kbps"],
    "wma": ["320 kbps", "256 kbps", "192 kbps", "128 kbps"],
}

QUALITY_MAP = {
    "320 kbps (CBR)": "320k", "256 kbps (CBR)": "256k",
    "192 kbps (CBR)": "192k", "128 kbps (CBR)": "128k",
    "VBR Высшее (Q0)": "0", "VBR Высокое (Q2)": "2",
    "VBR Среднее (Q4)": "4", "VBR Низкое (Q6)": "6",
}


def _dbl(val, mn, mx, step=0.01, dec=2) -> QDoubleSpinBox:
    s = QDoubleSpinBox()
    s.setRange(mn, mx)
    s.setSingleStep(step)
    s.setDecimals(dec)
    s.setValue(val)
    s.setFixedWidth(90)
    return s

def _int(val, mn, mx) -> QSpinBox:
    s = QSpinBox()
    s.setRange(mn, mx)
    s.setValue(val)
    s.setFixedWidth(80)
    return s

def _grp(title: str) -> QGroupBox:
    g = QGroupBox(title)
    lay = QVBoxLayout()
    lay.setSpacing(4)
    lay.setContentsMargins(6, 6, 6, 6)
    g.setLayout(lay)
    return g

def _row(*widgets) -> QWidget:
    w = QWidget()
    h = QHBoxLayout(w)
    h.setContentsMargins(0, 0, 0, 0)
    h.setSpacing(6)
    for ww in widgets:
        if ww is None:
            h.addStretch()
        else:
            h.addWidget(ww)
    return w

def _hline() -> QFrame:
    f = QFrame()
    f.setFrameShape(QFrame.Shape.HLine)
    f.setFrameShadow(QFrame.Shadow.Sunken)
    return f

def _sep() -> QFrame:
    return _hline()


class WorkerThread(QThread):
    progress      = pyqtSignal(int, int, str)
    file_done     = pyqtSignal(str, bool, str)
    all_done      = pyqtSignal(int, int)
    error_msg     = pyqtSignal(str)

    def __init__(self, files, tracks_info, output_dir, settings, metadata,
                 max_workers=1, delay_between=0.0, stop_event=None):
        super().__init__()
        self.files = files
        self.tracks_info = tracks_info
        self.output_dir = output_dir
        self.settings = settings
        self.metadata = metadata
        self.max_workers = max_workers
        self.delay_between = delay_between
        self._stop = stop_event or threading.Event()
        self._q: queue.Queue = queue.Queue()

    def run(self):
        if self.max_workers > 1:
            proc = BatchProcessor(
                files=self.files, tracks_info=self.tracks_info,
                output_dir=self.output_dir, settings=self.settings,
                metadata=self.metadata, result_queue=self._q,
                max_workers=self.max_workers, delay_between=self.delay_between,
                stop_event=self._stop,
            )
            t = threading.Thread(target=proc._run, daemon=True)
            t.start()
            while t.is_alive() or not self._q.empty():
                self._drain()
                time.sleep(0.05)
            self._drain()
        else:
            worker = ModificationWorker(
                files=self.files, tracks_info=self.tracks_info,
                output_dir=self.output_dir, settings=self.settings,
                metadata=self.metadata,
                on_progress=lambda c, t, fp: self.progress.emit(c, t, fp),
                on_file_complete=lambda fp, ok, out: self.file_done.emit(fp, ok, out),
                on_all_complete=lambda sc, tot: self.all_done.emit(sc, tot),
                on_error=lambda msg: self.error_msg.emit(msg),
                stop_event=self._stop,
            )
            worker.run()

    def _drain(self):
        while True:
            try:
                msg = self._q.get_nowait()
            except queue.Empty:
                break
            kind = msg[0]
            if kind == "progress":
                self.progress.emit(msg[1], msg[2], msg[3])
            elif kind == "file_done":
                self.file_done.emit(msg[1], msg[2], msg[3])
            elif kind == "all_done":
                self.all_done.emit(msg[1], msg[2])
            elif kind == "error":
                self.error_msg.emit(msg[1])


class ConverterThread(QThread):
    progress  = pyqtSignal(int, int, str)
    file_done = pyqtSignal(str, bool, str)
    all_done  = pyqtSignal(int, int)
    error_msg = pyqtSignal(str)

    def __init__(self, files, output_dir, output_format, quality_preset,
                 max_workers=4, delete_originals=False):
        super().__init__()
        self.files = files
        self.output_dir = output_dir
        self.output_format = output_format
        self.quality_preset = quality_preset
        self.max_workers = max_workers
        self.delete_originals = delete_originals
        self._q: queue.Queue = queue.Queue()

    def run(self):
        conv = BatchConverter(
            files=self.files, output_dir=self.output_dir,
            output_format=self.output_format, quality_preset=self.quality_preset,
            result_queue=self._q, max_workers=self.max_workers,
            delete_originals=self.delete_originals,
        )
        t = threading.Thread(target=conv._run, daemon=True)
        t.start()
        while t.is_alive() or not self._q.empty():
            self._drain()
            time.sleep(0.05)
        self._drain()

    def _drain(self):
        while True:
            try:
                msg = self._q.get_nowait()
            except queue.Empty:
                break
            kind = msg[0]
            if kind == "progress":
                self.progress.emit(msg[1], msg[2], msg[3])
            elif kind == "file_done":
                self.file_done.emit(msg[1], msg[2], msg[3])
            elif kind == "all_done":
                self.all_done.emit(msg[1], msg[2])
            elif kind == "error":
                self.error_msg.emit(msg[1])


class PreviewThread(QThread):
    done  = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, filepath, track_info, settings, metadata):
        super().__init__()
        self.filepath   = filepath
        self.track_info = track_info
        self.settings   = settings
        self.metadata   = metadata
        self._stop      = threading.Event()
        self._tmp_dir   = None

    def run(self):
        import shutil
        self._tmp_dir = tempfile.mkdtemp(prefix="vkmod_prev_")
        try:
            worker = ModificationWorker(
                files=[self.filepath],
                tracks_info=[self.track_info],
                output_dir=self._tmp_dir,
                settings=self.settings,
                metadata=self.metadata,
                on_progress=lambda *a: None,
                on_file_complete=self._on_complete,
                on_all_complete=lambda *a: None,
                on_error=lambda msg: self.error.emit(msg),
                stop_event=self._stop,
            )
            worker.run()
        except Exception as e:
            self.error.emit(str(e))

    def _on_complete(self, filepath, ok, output):
        if ok and output and os.path.exists(output):
            self.done.emit(output)
        else:
            self.error.emit("Предпросмотр не удался")

    def cancel(self):
        self._stop.set()

    def cleanup(self):
        if self._tmp_dir and os.path.isdir(self._tmp_dir):
            import shutil
            try:
                shutil.rmtree(self._tmp_dir, ignore_errors=True)
            except Exception:
                pass


class _TrackInfoLoader(QThread):
    loaded = pyqtSignal(str, object)

    def __init__(self, paths: list, parent=None):
        super().__init__(parent)
        self._paths = paths

    def run(self):
        for path in self._paths:
            try:
                ti = TrackInfo(path)
            except Exception:
                ti = None
            self.loaded.emit(path, ti)


class _WaveformLoader(QThread):
    loaded = pyqtSignal(str, object, object, float, float, float)

    def __init__(self, filepath: str, which: str, parent=None):
        super().__init__(parent)
        self._filepath = filepath
        self._which = which

    def run(self):
        try:
            import numpy as np
            sr = 22050
            result = subprocess.run(
                ["ffmpeg", "-i", self._filepath, "-vn", "-f", "f32le", "-ac", "1",
                 "-ar", str(sr), "pipe:1", "-y"],
                capture_output=True, stdin=subprocess.DEVNULL, timeout=30
            )
            if result.returncode != 0 or not result.stdout:
                return
            samples = np.frombuffer(result.stdout, dtype=np.float32)
            if len(samples) == 0:
                return
            duration = len(samples) / sr

            n_blocks = 4000
            factor = max(1, len(samples) // n_blocks)
            cut = (len(samples) // factor) * factor
            mat = samples[:cut].reshape(-1, factor)
            tops = mat.max(axis=1)
            bots = mat.min(axis=1)

            peak_abs = max(float(np.abs(tops).max()), float(np.abs(bots).max()), 1e-9)
            tops = tops / peak_abs
            bots = bots / peak_abs

            rms_db  = float(20 * np.log10(np.sqrt(np.mean(samples ** 2)) + 1e-9))
            peak_db = float(20 * np.log10(np.abs(samples).max() + 1e-9))

            self.loaded.emit(self._which, tops, bots, duration, rms_db, peak_db)
        except Exception:
            pass


class FileListPanel(QWidget):
    files_changed = pyqtSignal(list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._files: list[str] = []
        self._tracks: list[TrackInfo | None] = []
        self._loaders: list[_TrackInfoLoader] = []
        self._setup_ui()

    def _setup_ui(self):
        lay = QVBoxLayout(self)
        lay.setContentsMargins(4, 4, 4, 4)
        lay.setSpacing(4)

        btn_row = QWidget()
        bh = QHBoxLayout(btn_row)
        bh.setContentsMargins(0, 0, 0, 0)
        bh.setSpacing(4)

        self.btn_add = QPushButton("Добавить файлы")
        self.btn_add.clicked.connect(self._dialog_add)
        self.btn_recent = QPushButton("Недавние")
        self.btn_recent.clicked.connect(self._show_recent)
        self.btn_clear = QPushButton("Очистить")
        self.btn_clear.clicked.connect(self.clear)

        bh.addWidget(self.btn_add)
        bh.addWidget(self.btn_recent)
        bh.addWidget(self.btn_clear)
        lay.addWidget(btn_row)

        self.lst = QListWidget()
        self.lst.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.lst.setAcceptDrops(True)
        self.lst.setDragDropMode(QAbstractItemView.DragDropMode.DropOnly)
        self.lst.installEventFilter(self)
        self.lst.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.lst.customContextMenuRequested.connect(self._context_menu)
        lay.addWidget(self.lst)

        self.btn_remove = QPushButton("Удалить выбранные")
        self.btn_remove.clicked.connect(self._remove_selected)
        self.btn_remove.setEnabled(False)
        lay.addWidget(self.btn_remove)

        self.lbl_stats = QLabel("0 файлов | 0.0 MB")
        self.lbl_stats.setStyleSheet("color: gray; font-size: 9px;")
        lay.addWidget(self.lbl_stats)

        self.lst.itemSelectionChanged.connect(self._on_selection)

        self._recent: list[str] = []
        self.setAcceptDrops(True)

    def dragEnterEvent(self, e: QDragEnterEvent):
        if e.mimeData().hasUrls():
            e.acceptProposedAction()

    def dropEvent(self, e: QDropEvent):
        paths = [u.toLocalFile() for u in e.mimeData().urls()]
        self.add_files(paths)

    def eventFilter(self, obj, event):
        if obj is self.lst:
            from PyQt6.QtCore import QEvent
            if event.type() == QEvent.Type.DragEnter:
                if event.mimeData().hasUrls():
                    event.acceptProposedAction()
                    return True
            elif event.type() == QEvent.Type.Drop:
                paths = [u.toLocalFile() for u in event.mimeData().urls()]
                self.add_files(paths)
                return True
        return super().eventFilter(obj, event)

    def _dialog_add(self):
        paths, _ = QFileDialog.getOpenFileNames(
            self, "Выберите аудиофайлы", "",
            "Аудио (*.mp3 *.flac *.wav *.ogg *.aac *.m4a *.wma *.opus *.aiff *.alac *.wv *.ape *.tta *.ac3 *.dts *.mp2 *.mpc *.spx *.amr *.au *.mka *.oga *.caf *.shn)"
        )
        self.add_files(paths)

    def _show_recent(self):
        if not self._recent:
            QMessageBox.information(self, "Недавние файлы", "Список недавних файлов пуст.")
            return
        menu = QMenu(self)
        for p in self._recent[-20:]:
            act = menu.addAction(os.path.basename(p))
            act.setData(p)
        chosen = menu.exec(self.btn_recent.mapToGlobal(self.btn_recent.rect().bottomLeft()))
        if chosen and chosen.data():
            self.add_files([chosen.data()])

    def add_files(self, paths: list[str]):
        existing = set(self._files)
        added = []
        new_paths = []
        for p in paths:
            p = os.path.normpath(p)
            if not os.path.isfile(p):
                continue
            ext = os.path.splitext(p)[1].lower()
            if ext not in AUDIO_EXTENSIONS:
                continue
            if p in existing:
                continue
            existing.add(p)
            self._files.append(p)
            self._tracks.append(None)
            item = QListWidgetItem(os.path.basename(p))
            item.setData(Qt.ItemDataRole.UserRole, p)
            self.lst.addItem(item)
            added.append(p)
            new_paths.append(p)

        if added:
            self._recent = [p for p in self._recent if p not in added] + added
            self._recent = self._recent[-50:]
            self._update_stats()
            self.files_changed.emit(self._files)

            loader = _TrackInfoLoader(new_paths, self)
            loader.loaded.connect(self._on_track_info_loaded)
            loader.finished.connect(lambda l=loader: self._loaders.remove(l) if l in self._loaders else None)
            self._loaders.append(loader)
            loader.start()

    def _on_track_info_loaded(self, path: str, ti):
        try:
            idx = self._files.index(path)
        except ValueError:
            return
        self._tracks[idx] = ti
        item = self.lst.item(idx)
        if item and ti and ti.duration_sec:
            mins, secs = divmod(int(ti.duration_sec), 60)
            item.setText(os.path.basename(path) + f"  [{mins}:{secs:02d}]")

    def _remove_selected(self):
        rows = sorted({self.lst.row(i) for i in self.lst.selectedItems()}, reverse=True)
        for r in rows:
            self.lst.takeItem(r)
            self._files.pop(r)
            self._tracks.pop(r)
        self._update_stats()
        self.files_changed.emit(self._files)

    def clear(self):
        self.lst.clear()
        self._files.clear()
        self._tracks.clear()
        self._update_stats()
        self.files_changed.emit(self._files)

    def color_item(self, filepath: str, ok: bool):
        for i in range(self.lst.count()):
            item = self.lst.item(i)
            if item.data(Qt.ItemDataRole.UserRole) == filepath:
                item.setForeground(QColor("#22cc44" if ok else "#cc2222"))
                break

    def _on_selection(self):
        has = bool(self.lst.selectedItems())
        self.btn_remove.setEnabled(has)

    def _context_menu(self, pos):
        menu = QMenu(self)
        menu.addAction("Добавить файлы", self._dialog_add)
        menu.addAction("Удалить выбранные", self._remove_selected)
        menu.addSeparator()
        menu.addAction("Очистить список", self.clear)
        menu.exec(self.lst.mapToGlobal(pos))

    def _update_stats(self):
        n = len(self._files)
        mb = sum(os.path.getsize(p) for p in self._files if os.path.exists(p)) / 1_048_576
        self.lbl_stats.setText(f"{n} файлов | {mb:.1f} MB")

    def get_files(self) -> list[str]:
        return list(self._files)

    def get_tracks(self) -> list:
        return list(self._tracks)

    def current_track(self) -> "TrackInfo | None":
        sel = self.lst.selectedItems()
        if not sel:
            return None
        r = self.lst.row(sel[0])
        return self._tracks[r] if r < len(self._tracks) else None

    def current_file(self) -> "str | None":
        sel = self.lst.selectedItems()
        if not sel:
            return None
        r = self.lst.row(sel[0])
        return self._files[r] if r < len(self._files) else None


class CoverWidget(QGroupBox):
    changed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__("Обложка", parent)
        self._path: str | None = None
        self._temp_path: str | None = None
        lay = QVBoxLayout(self)
        lay.setSpacing(4)

        self.lbl_img = QLabel("(нет)")
        self.lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_img.setFixedSize(100, 100)
        self.lbl_img.setStyleSheet("border: 1px solid #555; background: #222;")
        lay.addWidget(self.lbl_img, 0, Qt.AlignmentFlag.AlignHCenter)

        lay.addWidget(QPushButton("Загрузить", clicked=self._load))
        lay.addWidget(QPushButton("Рандом", clicked=self._random))
        self.btn_rm = QPushButton("Удалить")
        self.btn_rm.setEnabled(False)
        self.btn_rm.clicked.connect(self._remove)
        lay.addWidget(self.btn_rm)

    def _load(self):
        p, _ = QFileDialog.getOpenFileName(
            self, "Выберите обложку", "",
            "Изображения (*.jpg *.jpeg *.png *.bmp *.webp)"
        )
        if p:
            self._set_path(p)

    def _random(self):
        img = QImage(256, 256, QImage.Format.Format_RGB888)
        img.fill(QColor(random.randint(30, 220), random.randint(30, 220), random.randint(30, 220)))
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        tmp.close()
        if self._temp_path and os.path.exists(self._temp_path):
            try:
                os.unlink(self._temp_path)
            except Exception:
                pass
        QPixmap.fromImage(img).save(tmp.name, "PNG")
        self._temp_path = tmp.name
        self._set_path(tmp.name)

    def _remove(self):
        self._path = None
        self.lbl_img.setText("(нет)")
        self.lbl_img.setPixmap(QPixmap())
        self.btn_rm.setEnabled(False)
        self.changed.emit()

    def _set_path(self, path: str):
        self._path = path
        px = QPixmap(path).scaled(100, 100, Qt.AspectRatioMode.KeepAspectRatio,
                                   Qt.TransformationMode.SmoothTransformation)
        self.lbl_img.setPixmap(px)
        self.btn_rm.setEnabled(True)
        self.changed.emit()

    def set_from_track(self, track: "TrackInfo | None"):
        if track and track.cover_data:
            img = QImage.fromData(track.cover_data)
            if not img.isNull():
                px = QPixmap.fromImage(img).scaled(100, 100,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation)
                self.lbl_img.setPixmap(px)
                self.btn_rm.setEnabled(True)
                return
        self._remove()

    def get_path(self) -> "str | None":
        return self._path


class MetadataWidget(QGroupBox):
    def __init__(self, parent=None):
        super().__init__("Метаданные", parent)
        lay = QGridLayout(self)
        lay.setSpacing(4)
        labels = ["Название", "Исполнитель", "Альбом", "Год", "Жанр"]
        self._fields: dict[str, QLineEdit] = {}
        for i, lbl in enumerate(labels):
            lay.addWidget(QLabel(lbl), i, 0)
            e = QLineEdit()
            lay.addWidget(e, i, 1)
            self._fields[lbl] = e
        lay.setColumnStretch(1, 1)
        self._track = None

    def set_from_track(self, track, force: bool = False):
        self._track = track
        if track:
            self._fields["Название"].setText(track.title or "")
            self._fields["Исполнитель"].setText(track.artist or "")
            self._fields["Альбом"].setText(track.album or "")
            self._fields["Год"].setText(str(track.year or ""))
            self._fields["Жанр"].setText(track.genre or "")
        else:
            for f in self._fields.values():
                f.setText("")

    def get_values(self) -> dict:
        return {"title": "", "artist": "", "album": "", "year": "", "genre": ""}

    def get_lock_states(self) -> dict:
        return {}

    def set_values(self, d: dict):
        pass


class TrackInfoWidget(QGroupBox):
    def __init__(self, parent=None):
        super().__init__("Анализ трека", parent)
        lay = QVBoxLayout(self)
        self.txt = QTextEdit()
        self.txt.setReadOnly(True)
        self.txt.setFont(QFont("Consolas", 9))
        self.txt.setFixedWidth(200)
        lay.addWidget(self.txt)
        self.show_track(None)

    def show_track(self, track: "TrackInfo | None"):
        if track is None:
            self.txt.setPlainText("(нет файла)")
            return
        lines = [
            f"Файл:     {track.file_name}",
            f"Размер:   {track.size_mb:.2f} MB",
            f"Длит.:    {int(track.duration_sec // 60)}:{int(track.duration_sec % 60):02d}",
            f"Битрейт:  {track.bitrate} kbps",
            f"Частота:  {track.sample_rate} Hz",
            f"Название: {track.title}",
            f"Артист:   {track.artist}",
            f"Альбом:   {track.album}",
        ]
        self.txt.setPlainText("\n".join(lines))


class BasicTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)

        content = QWidget()
        scroll.setWidget(content)
        lay = QVBoxLayout(content)
        lay.setSpacing(6)

        g = _grp("Сдвиг тона (полутоны)")
        self.cb_pitch = QCheckBox("Включить")
        self.sp_pitch = _dbl(0.5, -12.0, 12.0, 0.5, 1)
        g.layout().addWidget(self.cb_pitch)
        g.layout().addWidget(_row(QLabel("Полутонов:"), self.sp_pitch, None))
        lay.addWidget(g)

        g = _grp("Изменение темпа")
        self.cb_speed = QCheckBox("Включить")
        self.sp_speed = _dbl(1.00, 0.5, 2.0, 0.05, 2)
        g.layout().addWidget(self.cb_speed)
        g.layout().addWidget(_row(QLabel("Коэффициент:"), self.sp_speed, None))
        lay.addWidget(g)

        g = _grp("Эквалайзер")
        self.cb_eq = QCheckBox("Включить")
        self.cmb_eq = QComboBox()
        self.cmb_eq.addItems(["Пользовательский", "Срез мидов (1–2 кГц)", "Подъём высоких (8 кГц)"])
        self.sp_eq = _dbl(-2.0, -20.0, 20.0, 0.5, 1)
        g.layout().addWidget(self.cb_eq)
        g.layout().addWidget(_row(QLabel("Тип:"), self.cmb_eq))
        g.layout().addWidget(_row(QLabel("Усиление (dB):"), self.sp_eq, None))
        lay.addWidget(g)

        g = _grp("Добавить тишину в конец")
        self.cb_silence = QCheckBox("Включить")
        self.sp_silence = _int(45, 1, 300)
        g.layout().addWidget(self.cb_silence)
        g.layout().addWidget(_row(QLabel("Секунд:"), self.sp_silence, None))
        lay.addWidget(g)

        g = _grp("Нормализация громкости (EBU R128)")
        self.cb_loudnorm = QCheckBox("Включить")
        self.sp_loudnorm = _dbl(-14.0, -35.0, -5.0, 0.5, 1)
        g.layout().addWidget(self.cb_loudnorm)
        g.layout().addWidget(_row(QLabel("Цель LUFS:"), self.sp_loudnorm, None))
        lay.addWidget(g)

        lay.addStretch()

    def get_values(self) -> dict:
        return {
            "pitch": self.cb_pitch.isChecked(),
            "pitch_value": self.sp_pitch.value(),
            "speed": self.cb_speed.isChecked(),
            "speed_value": self.sp_speed.value(),
            "eq": self.cb_eq.isChecked(),
            "eq_type": self.cmb_eq.currentIndex(),
            "eq_value": self.sp_eq.value(),
            "silence": self.cb_silence.isChecked(),
            "silence_duration": self.sp_silence.value(),
            "loudnorm": self.cb_loudnorm.isChecked(),
            "loudnorm_target": self.sp_loudnorm.value(),
        }

    def set_values(self, d: dict):
        self.cb_pitch.setChecked(d.get("pitch", False))
        self.sp_pitch.setValue(d.get("pitch_value", 0.5))
        self.cb_speed.setChecked(d.get("speed", False))
        self.sp_speed.setValue(d.get("speed_value", 1.0))
        self.cb_eq.setChecked(d.get("eq", False))
        self.cmb_eq.setCurrentIndex(d.get("eq_type", 0))
        self.sp_eq.setValue(d.get("eq_value", -2.0))
        self.cb_silence.setChecked(d.get("silence", False))
        self.sp_silence.setValue(int(d.get("silence_duration", 45)))
        self.cb_loudnorm.setChecked(d.get("loudnorm", False))
        self.sp_loudnorm.setValue(d.get("loudnorm_target", -14.0))


class SpectralTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)

        content = QWidget()
        scroll.setWidget(content)
        lay = QVBoxLayout(content)
        lay.setSpacing(6)

        g = _grp("Инверсия фазы")
        self.cb_phase_inv = QCheckBox("Включить")
        self.sp_phase_inv = _dbl(1.0, 0.1, 1.0, 0.1, 1)
        g.layout().addWidget(self.cb_phase_inv)
        g.layout().addWidget(_row(QLabel("Сила (0–1):"), self.sp_phase_inv, None))
        lay.addWidget(g)

        g = _grp("Фазовый скремблинг")
        self.cb_phase_scr = QCheckBox("Включить")
        self.sp_phase_scr = _dbl(2.0, 0.1, 20.0, 0.1, 1)
        g.layout().addWidget(self.cb_phase_scr)
        g.layout().addWidget(_row(QLabel("Скорость:"), self.sp_phase_scr, None))
        lay.addWidget(g)

        g = _grp("DC-сдвиг")
        self.cb_dc = QCheckBox("Включить")
        self.sp_dc = _dbl(0.000005, 0.000001, 0.01, 0.000001, 6)
        g.layout().addWidget(self.cb_dc)
        g.layout().addWidget(_row(QLabel("Смещение:"), self.sp_dc, None))
        lay.addWidget(g)

        g = _grp("Дрейф частоты дискретизации")
        self.cb_resamp = QCheckBox("Включить")
        self.sp_resamp = _int(1, -100, 100)
        g.layout().addWidget(self.cb_resamp)
        g.layout().addWidget(_row(QLabel("Дрейф (Гц):"), self.sp_resamp, None))
        lay.addWidget(g)

        g = _grp("Haas-задержка (стерео расширение)")
        self.cb_haas = QCheckBox("Включить")
        self.sp_haas = _dbl(15.0, 1.0, 40.0, 0.5, 1)
        g.layout().addWidget(self.cb_haas)
        g.layout().addWidget(_row(QLabel("Задержка (мс):"), self.sp_haas, None))
        lay.addWidget(g)

        g = _grp("Ультразвуковой шум")
        self.cb_ultra = QCheckBox("Включить")
        self.sp_ultra_freq = _int(21000, 18000, 22000)
        self.sp_ultra_level = _dbl(0.001, 0.0001, 0.01, 0.0001, 4)
        g.layout().addWidget(self.cb_ultra)
        g.layout().addWidget(_row(QLabel("Частота (Гц):"), self.sp_ultra_freq, None))
        g.layout().addWidget(_row(QLabel("Уровень:"), self.sp_ultra_level, None))
        lay.addWidget(g)

        g = _grp("Дизеринг-атака")
        self.cb_dither = QCheckBox("Включить")
        self.cmb_dither = QComboBox()
        self.cmb_dither.addItems(["triangular_hp", "triangular", "rectangular", "shaped"])
        g.layout().addWidget(self.cb_dither)
        g.layout().addWidget(_row(QLabel("Метод:"), self.cmb_dither))
        lay.addWidget(g)

        g = _grp("ID3 Padding Attack")
        self.cb_id3pad = QCheckBox("Включить")
        self.sp_id3pad = _int(512, 64, 2048)
        g.layout().addWidget(self.cb_id3pad)
        g.layout().addWidget(_row(QLabel("Байт:"), self.sp_id3pad, None))
        lay.addWidget(g)

        lay.addStretch()

    def get_values(self) -> dict:
        return {
            "phase_invert": self.cb_phase_inv.isChecked(),
            "phase_invert_strength": self.sp_phase_inv.value(),
            "phase_scramble": self.cb_phase_scr.isChecked(),
            "phase_scramble_speed": self.sp_phase_scr.value(),
            "dc_shift": self.cb_dc.isChecked(),
            "dc_shift_value": self.sp_dc.value(),
            "resample_drift": self.cb_resamp.isChecked(),
            "resample_drift_amount": self.sp_resamp.value(),
            "haas_delay": self.cb_haas.isChecked(),
            "haas_delay_ms": self.sp_haas.value(),
            "ultrasonic_noise": self.cb_ultra.isChecked(),
            "ultrasonic_freq": self.sp_ultra_freq.value(),
            "ultrasonic_level": self.sp_ultra_level.value(),
            "dither_attack": self.cb_dither.isChecked(),
            "dither_method": self.cmb_dither.currentText(),
            "id3_padding_attack": self.cb_id3pad.isChecked(),
            "id3_padding_bytes": self.sp_id3pad.value(),
        }

    def set_values(self, d: dict):
        self.cb_phase_inv.setChecked(d.get("phase_invert", False))
        self.sp_phase_inv.setValue(d.get("phase_invert_strength", 1.0))
        self.cb_phase_scr.setChecked(d.get("phase_scramble", False))
        self.sp_phase_scr.setValue(d.get("phase_scramble_speed", 2.0))
        self.cb_dc.setChecked(d.get("dc_shift", False))
        self.sp_dc.setValue(d.get("dc_shift_value", 0.000005))
        self.cb_resamp.setChecked(d.get("resample_drift", False))
        self.sp_resamp.setValue(int(d.get("resample_drift_amount", 1)))
        self.cb_haas.setChecked(d.get("haas_delay", False))
        self.sp_haas.setValue(d.get("haas_delay_ms", 15.0))
        self.cb_ultra.setChecked(d.get("ultrasonic_noise", False))
        self.sp_ultra_freq.setValue(int(d.get("ultrasonic_freq", 21000)))
        self.sp_ultra_level.setValue(d.get("ultrasonic_level", 0.001))
        self.cb_dither.setChecked(d.get("dither_attack", False))
        idx = self.cmb_dither.findText(d.get("dither_method", "triangular_hp"))
        if idx >= 0:
            self.cmb_dither.setCurrentIndex(idx)
        self.cb_id3pad.setChecked(d.get("id3_padding_attack", False))
        self.sp_id3pad.setValue(int(d.get("id3_padding_bytes", 512)))


class TextureTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)

        content = QWidget()
        scroll.setWidget(content)
        lay = QVBoxLayout(content)
        lay.setSpacing(6)

        g = _grp("Спектральное маскирование")
        self.cb_spec_mask = QCheckBox("Включить")
        self.sp_mask_sens = _dbl(0.8, 0.1, 3.0, 0.1, 1)
        self.sp_mask_att = _int(12, 1, 30)
        self.sp_mask_peaks = _int(10, 1, 27)
        g.layout().addWidget(self.cb_spec_mask)
        g.layout().addWidget(_row(QLabel("Чувств.:"), self.sp_mask_sens, None))
        g.layout().addWidget(_row(QLabel("Ослабл. (dB):"), self.sp_mask_att, None))
        g.layout().addWidget(_row(QLabel("Пиков макс.:"), self.sp_mask_peaks, None))
        lay.addWidget(g)

        g = _grp("Концертная эмуляция")
        self.cb_concert = QCheckBox("Включить")
        self.cmb_concert = QComboBox()
        self.cmb_concert.addItems(["light", "medium", "heavy"])
        self.cmb_concert.setCurrentIndex(1)
        g.layout().addWidget(self.cb_concert)
        g.layout().addWidget(_row(QLabel("Интенсивность:"), self.cmb_concert))
        lay.addWidget(g)

        g = _grp("Mid/Side обработка")
        self.cb_midside = QCheckBox("Включить")
        self.sp_mid_gain = _dbl(-3.0, -20.0, 20.0, 0.5, 1)
        self.sp_side_gain = _dbl(2.0, -20.0, 20.0, 0.5, 1)
        g.layout().addWidget(self.cb_midside)
        g.layout().addWidget(_row(QLabel("Mid (dB):"), self.sp_mid_gain, None))
        g.layout().addWidget(_row(QLabel("Side (dB):"), self.sp_side_gain, None))
        lay.addWidget(g)

        g = _grp("Психоакустическая диффузия")
        self.cb_psycho = QCheckBox("Включить")
        self.sp_psycho = _dbl(0.0003, 0.00001, 0.005, 0.00001, 5)
        g.layout().addWidget(self.cb_psycho)
        g.layout().addWidget(_row(QLabel("Интенсивность:"), self.sp_psycho, None))
        lay.addWidget(g)

        g = _grp("Аналоговое насыщение")
        self.cb_sat = QCheckBox("Включить")
        self.sp_sat_drive = _dbl(1.5, 1.0, 5.0, 0.1, 1)
        self.sp_sat_mix = _dbl(0.15, 0.0, 1.0, 0.01, 2)
        g.layout().addWidget(self.cb_sat)
        g.layout().addWidget(_row(QLabel("Drive:"), self.sp_sat_drive, None))
        g.layout().addWidget(_row(QLabel("Mix:"), self.sp_sat_mix, None))
        lay.addWidget(g)

        g = _grp("Временной джиттер")
        self.cb_temp_jitter = QCheckBox("Включить")
        self.sp_jitter_int = _dbl(0.002, 0.0001, 0.1, 0.0001, 4)
        self.sp_jitter_freq = _dbl(0.5, 0.1, 20.0, 0.1, 1)
        g.layout().addWidget(self.cb_temp_jitter)
        g.layout().addWidget(_row(QLabel("Интенсивность:"), self.sp_jitter_int, None))
        g.layout().addWidget(_row(QLabel("Частота (Гц):"), self.sp_jitter_freq, None))
        lay.addWidget(g)

        g = _grp("Спектральный джиттер (notch-фильтры)")
        self.cb_spec_jitter = QCheckBox("Включить")
        self.cmb_sj_mode = QComboBox()
        self.cmb_sj_mode.addItems(["Случайные", "Конструктор"])
        self.sp_sj_count = _int(5, 1, 16)
        self.sp_sj_att = _dbl(15.0, 1.0, 40.0, 0.5, 1)
        g.layout().addWidget(self.cb_spec_jitter)
        g.layout().addWidget(_row(QLabel("Режим:"), self.cmb_sj_mode))
        g.layout().addWidget(_row(QLabel("Кол-во нотчей:"), self.sp_sj_count, None))
        g.layout().addWidget(_row(QLabel("Ослабл. (dB):"), self.sp_sj_att, None))

        self._sj_constructor = QWidget()
        sj_lay = QVBoxLayout(self._sj_constructor)
        sj_lay.setContentsMargins(0, 4, 0, 0)
        sj_lay.setSpacing(4)

        _SJ_PRESETS = [
            ("Мягкий",
             [(630, 3.0, 1.5), (3150, 4.0, 1.5)]),
            ("Лёгкий",
             [(400, 5.0, 2.0), (1600, 6.0, 2.0), (6300, 5.0, 2.0)]),
            ("Средний",
             [(250, 8.0, 2.5), (800, 9.0, 2.5), (2500, 8.0, 2.5),
              (8000, 7.0, 2.0), (12500, 6.0, 1.5)]),
            ("Сильный",
             [(120, 12.0, 3.0), (400, 14.0, 3.0), (1000, 15.0, 3.0),
              (3150, 14.0, 3.0), (6300, 12.0, 2.5), (10000, 10.0, 2.0)]),
            ("Жёсткий",
             [(120, 18.0, 4.0), (250, 20.0, 4.0), (630, 22.0, 4.0),
              (1200, 20.0, 4.0), (2500, 20.0, 3.5), (4000, 18.0, 3.5),
              (8000, 18.0, 3.0), (12500, 15.0, 3.0)]),
            ("Максимум",
             [(120, 25.0, 5.0), (250, 28.0, 5.0), (400, 30.0, 5.0),
              (800, 28.0, 5.0), (1200, 30.0, 5.0), (2000, 28.0, 4.5),
              (3150, 28.0, 4.5), (5000, 25.0, 4.0), (8000, 25.0, 4.0),
              (10000, 22.0, 4.0), (12500, 20.0, 3.5)]),
        ]
        pr_row = QWidget()
        pr_lay = QHBoxLayout(pr_row)
        pr_lay.setContentsMargins(0, 0, 0, 0)
        pr_lay.setSpacing(3)
        pr_lay.addWidget(QLabel("Пресет:"))
        for _pname, _pdata in _SJ_PRESETS:
            _btn_p = QPushButton(_pname)
            _btn_p.setFixedHeight(22)
            _btn_p.setStyleSheet("font-size:9px;padding:0 4px;")
            _btn_p.clicked.connect(
                lambda checked=False, d=_pdata: self._apply_sj_preset(d)
            )
            pr_lay.addWidget(_btn_p)
        pr_lay.addStretch()
        sj_lay.addWidget(pr_row)
        sj_lay.addWidget(_hline())

        hdr_row = QWidget()
        hdr_lay = QHBoxLayout(hdr_row)
        hdr_lay.setContentsMargins(0, 0, 0, 0)
        hdr_lay.setSpacing(4)
        for _htxt in ("Частота (Гц)", "Ослаб. (dB)", "Q"):
            _hl = QLabel(_htxt)
            _hl.setFixedWidth(90)
            _hl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            _hl.setStyleSheet("color:#888;font-size:9px;")
            hdr_lay.addWidget(_hl)
        hdr_lay.addSpacing(28)
        hdr_lay.addStretch()
        sj_lay.addWidget(hdr_row)

        self._sj_rows_widget = QWidget()
        self._sj_rows_lay = QVBoxLayout(self._sj_rows_widget)
        self._sj_rows_lay.setContentsMargins(0, 0, 0, 0)
        self._sj_rows_lay.setSpacing(2)
        sj_lay.addWidget(self._sj_rows_widget)
        btn_add_sj = QPushButton("+ Добавить фильтр")
        btn_add_sj.clicked.connect(self._add_sj_row)
        sj_lay.addWidget(btn_add_sj)
        g.layout().addWidget(self._sj_constructor)
        self._sj_rows: list[dict] = []
        self._sj_constructor.hide()
        self.cmb_sj_mode.currentIndexChanged.connect(self._on_sj_mode)
        lay.addWidget(g)

        g = _grp("VK Инфразвук")
        self.cb_infra = QCheckBox("Включить")
        self.cmb_infra_mode = QComboBox()
        self.cmb_infra_mode.addItems(["simple", "modulated", "phase", "harmonic", "maximum"])
        self.cmb_infra_mode.setCurrentIndex(1)
        self.cmb_infra_wave = QComboBox()
        self.cmb_infra_wave.addItems(["sine", "triangle", "square"])
        self.sp_infra_freq = _dbl(18.0, 1.0, 20.0, 0.5, 1)
        self.sp_infra_amp = _dbl(0.35, 0.01, 1.0, 0.01, 2)
        self.sp_infra_mod_freq = _dbl(0.08, 0.01, 5.0, 0.01, 2)
        self.sp_infra_mod_depth = _dbl(0.3, 0.0, 1.0, 0.05, 2)
        self.sp_infra_phase = _dbl(0.0, 0.0, 6.28, 0.01, 2)
        self.cb_infra_adaptive = QCheckBox("Адаптивная амплитуда")
        self.cb_infra_adaptive.setChecked(True)
        self.sp_infra_h1 = _dbl(0.15, 0.0, 1.0, 0.01, 2)
        self.sp_infra_h2 = _dbl(0.07, 0.0, 1.0, 0.01, 2)
        self.sp_infra_h3 = _dbl(0.03, 0.0, 1.0, 0.01, 2)

        g.layout().addWidget(self.cb_infra)
        g.layout().addWidget(_row(QLabel("Режим:"), self.cmb_infra_mode))
        g.layout().addWidget(_row(QLabel("Форма волны:"), self.cmb_infra_wave))
        g.layout().addWidget(_row(QLabel("Частота (Гц):"), self.sp_infra_freq, None))
        g.layout().addWidget(_row(QLabel("Амплитуда:"), self.sp_infra_amp, None))
        g.layout().addWidget(_row(QLabel("Частота мод.:"), self.sp_infra_mod_freq, None))
        g.layout().addWidget(_row(QLabel("Глубина мод.:"), self.sp_infra_mod_depth, None))
        g.layout().addWidget(_row(QLabel("Фаза (рад.):"), self.sp_infra_phase, None))
        g.layout().addWidget(self.cb_infra_adaptive)
        g.layout().addWidget(QLabel("Гармоники (h2, h3, h4):"))
        g.layout().addWidget(_row(self.sp_infra_h1, self.sp_infra_h2, self.sp_infra_h3, None))
        lay.addWidget(g)

        lay.addStretch()

    def _on_sj_mode(self, idx):
        is_constructor = (idx == 1)
        self._sj_constructor.setVisible(is_constructor)
        self.sp_sj_count.setEnabled(not is_constructor)

    def _add_sj_row(self, freq=1000.0, att=15.0, width=2.0):
        row_w = QWidget()
        rh = QHBoxLayout(row_w)
        rh.setContentsMargins(0, 0, 0, 0)
        rh.setSpacing(4)
        sp_f = _dbl(freq, 20.0, 20000.0, 50.0, 0)
        sp_a = _dbl(att, 1.0, 40.0, 0.5, 1)
        sp_w = _dbl(width, 0.5, 10.0, 0.1, 1)
        btn_del = QPushButton("✕")
        btn_del.setFixedWidth(24)
        rh.addWidget(sp_f)
        rh.addWidget(sp_a)
        rh.addWidget(sp_w)
        rh.addWidget(btn_del)
        rh.addStretch()
        rdata = {"freq": sp_f, "att": sp_a, "width": sp_w, "widget": row_w}
        self._sj_rows.append(rdata)
        self._sj_rows_lay.addWidget(row_w)
        btn_del.clicked.connect(lambda: self._del_sj_row(rdata))

    def _del_sj_row(self, rdata):
        if rdata in self._sj_rows:
            self._sj_rows.remove(rdata)
        rdata["widget"].setParent(None)
        rdata["widget"].deleteLater()

    def _apply_sj_preset(self, rows: list):
        for rdata in list(self._sj_rows):
            rdata["widget"].setParent(None)
            rdata["widget"].deleteLater()
        self._sj_rows.clear()
        for freq, att, q in rows:
            self._add_sj_row(freq, att, q)
        self.cmb_sj_mode.setCurrentIndex(1)

    def get_values(self) -> dict:
        mode = self.cmb_sj_mode.currentIndex()
        if mode == 1 and self._sj_rows:
            manual_config = {
                "mode": "manual",
                "frequencies": [r["freq"].value() for r in self._sj_rows],
                "attenuations": [r["att"].value() for r in self._sj_rows],
                "widths": [r["width"].value() for r in self._sj_rows],
            }
            fixed_freqs = manual_config["frequencies"]
            fixed_att = None
        else:
            manual_config = {"mode": "random"}
            fixed_freqs = None
            fixed_att = None

        return {
            "spectral_masking": self.cb_spec_mask.isChecked(),
            "spectral_mask_sensitivity": self.sp_mask_sens.value(),
            "spectral_mask_attenuation": self.sp_mask_att.value(),
            "spectral_mask_peaks": self.sp_mask_peaks.value(),
            "concert_emulation": self.cb_concert.isChecked(),
            "concert_intensity": self.cmb_concert.currentText(),
            "midside_processing": self.cb_midside.isChecked(),
            "midside_mid_gain": self.sp_mid_gain.value(),
            "midside_side_gain": self.sp_side_gain.value(),
            "psychoacoustic_noise": self.cb_psycho.isChecked(),
            "psychoacoustic_intensity": self.sp_psycho.value(),
            "saturation": self.cb_sat.isChecked(),
            "saturation_drive": self.sp_sat_drive.value(),
            "saturation_mix": self.sp_sat_mix.value(),
            "temporal_jitter": self.cb_temp_jitter.isChecked(),
            "jitter_intensity": self.sp_jitter_int.value(),
            "jitter_frequency": self.sp_jitter_freq.value(),
            "spectral_jitter": self.cb_spec_jitter.isChecked(),
            "spectral_jitter_count": self.sp_sj_count.value(),
            "spectral_jitter_attenuation": self.sp_sj_att.value(),
            "spectral_jitter_fixed_frequencies": fixed_freqs,
            "spectral_jitter_fixed_attenuation": fixed_att,
            "spectral_jitter_manual_config": manual_config,
            "_sj_rows": [
                {"freq": r["freq"].value(), "att": r["att"].value(), "width": r["width"].value()}
                for r in self._sj_rows
            ],
            "vk_infrasonic": self.cb_infra.isChecked(),
            "vk_infrasonic_mode": self.cmb_infra_mode.currentText(),
            "vk_infrasonic_waveform": self.cmb_infra_wave.currentText(),
            "vk_infrasonic_freq": self.sp_infra_freq.value(),
            "vk_infrasonic_amplitude": self.sp_infra_amp.value(),
            "vk_infrasonic_mod_freq": self.sp_infra_mod_freq.value(),
            "vk_infrasonic_mod_depth": self.sp_infra_mod_depth.value(),
            "vk_infrasonic_phase_shift": self.sp_infra_phase.value(),
            "vk_infrasonic_adaptive_amplitude": self.cb_infra_adaptive.isChecked(),
            "vk_infrasonic_harmonics": [
                self.sp_infra_h1.value(),
                self.sp_infra_h2.value(),
                self.sp_infra_h3.value(),
            ],
        }

    def set_values(self, d: dict):
        self.cb_spec_mask.setChecked(d.get("spectral_masking", False))
        self.sp_mask_sens.setValue(d.get("spectral_mask_sensitivity", 0.8))
        self.sp_mask_att.setValue(int(d.get("spectral_mask_attenuation", 12)))
        self.sp_mask_peaks.setValue(int(d.get("spectral_mask_peaks", 10)))
        self.cb_concert.setChecked(d.get("concert_emulation", False))
        idx = self.cmb_concert.findText(d.get("concert_intensity", "medium"))
        if idx >= 0:
            self.cmb_concert.setCurrentIndex(idx)
        self.cb_midside.setChecked(d.get("midside_processing", False))
        self.sp_mid_gain.setValue(d.get("midside_mid_gain", -3.0))
        self.sp_side_gain.setValue(d.get("midside_side_gain", 2.0))
        self.cb_psycho.setChecked(d.get("psychoacoustic_noise", False))
        self.sp_psycho.setValue(d.get("psychoacoustic_intensity", 0.0003))
        self.cb_sat.setChecked(d.get("saturation", False))
        self.sp_sat_drive.setValue(d.get("saturation_drive", 1.5))
        self.sp_sat_mix.setValue(d.get("saturation_mix", 0.15))
        self.cb_temp_jitter.setChecked(d.get("temporal_jitter", False))
        self.sp_jitter_int.setValue(d.get("jitter_intensity", 0.002))
        self.sp_jitter_freq.setValue(d.get("jitter_frequency", 0.5))
        self.cb_spec_jitter.setChecked(d.get("spectral_jitter", False))
        self.sp_sj_count.setValue(int(d.get("spectral_jitter_count", 5)))
        self.sp_sj_att.setValue(d.get("spectral_jitter_attenuation", 15.0))
        rows_data = d.get("_sj_rows", [])
        for r in rows_data:
            self._add_sj_row(r.get("freq", 1000), r.get("att", 15), r.get("width", 2))
        if rows_data:
            self.cmb_sj_mode.setCurrentIndex(1)
        self.cb_infra.setChecked(d.get("vk_infrasonic", False))
        idx = self.cmb_infra_mode.findText(d.get("vk_infrasonic_mode", "modulated"))
        if idx >= 0:
            self.cmb_infra_mode.setCurrentIndex(idx)
        idx = self.cmb_infra_wave.findText(d.get("vk_infrasonic_waveform", "sine"))
        if idx >= 0:
            self.cmb_infra_wave.setCurrentIndex(idx)
        self.sp_infra_freq.setValue(d.get("vk_infrasonic_freq", 18.0))
        self.sp_infra_amp.setValue(d.get("vk_infrasonic_amplitude", 0.35))
        self.sp_infra_mod_freq.setValue(d.get("vk_infrasonic_mod_freq", 0.08))
        self.sp_infra_mod_depth.setValue(d.get("vk_infrasonic_mod_depth", 0.3))
        self.sp_infra_phase.setValue(d.get("vk_infrasonic_phase_shift", 0.0))
        self.cb_infra_adaptive.setChecked(d.get("vk_infrasonic_adaptive_amplitude", True))
        harmonics = d.get("vk_infrasonic_harmonics", [0.15, 0.07, 0.03])
        if len(harmonics) > 0:
            self.sp_infra_h1.setValue(harmonics[0])
        if len(harmonics) > 1:
            self.sp_infra_h2.setValue(harmonics[1])
        if len(harmonics) > 2:
            self.sp_infra_h3.setValue(harmonics[2])


class AdvancedTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)

        content = QWidget()
        scroll.setWidget(content)
        lay = QVBoxLayout(content)
        lay.setSpacing(6)

        g = _grp("Обрезка начальной тишины")
        self.cb_trim = QCheckBox("Включить")
        self.sp_trim = _dbl(5.0, 0.1, 60.0, 0.5, 1)
        self.sp_trim_thresh = _int(-60, -90, -20)
        g.layout().addWidget(self.cb_trim)
        g.layout().addWidget(_row(QLabel("Макс. обрезать (с):"), self.sp_trim, None))
        g.layout().addWidget(_row(QLabel("Порог тишины (dBFS):"), self.sp_trim_thresh, None))
        g.layout().addWidget(QLabel("silencedetect находит реальный конец тишины", styleSheet="color:gray;font-size:9px;"))
        lay.addWidget(g)

        g = _grp("Вырезать фрагмент")
        self.cb_cut = QCheckBox("Включить")
        self.sp_cut_pos = _int(50, 0, 100)
        self.sp_cut_dur = _dbl(2.0, 0.1, 30.0, 0.1, 1)
        g.layout().addWidget(self.cb_cut)
        g.layout().addWidget(_row(QLabel("Позиция (%):"), self.sp_cut_pos, None))
        g.layout().addWidget(_row(QLabel("Длина (с):"), self.sp_cut_dur, None))
        lay.addWidget(g)

        g = _grp("Затухание в конце (Fade Out)")
        self.cb_fade = QCheckBox("Включить")
        self.sp_fade = _dbl(5.0, 0.5, 60.0, 0.5, 1)
        g.layout().addWidget(self.cb_fade)
        g.layout().addWidget(_row(QLabel("Длина (с):"), self.sp_fade, None))
        lay.addWidget(g)

        g = _grp("Объединить с треком")
        self.cb_merge = QCheckBox("Включить")
        self.le_extra = QLineEdit()
        self.le_extra.setPlaceholderText("Путь к файлу...")
        btn_browse = QPushButton("…")
        btn_browse.setFixedWidth(28)
        btn_browse.clicked.connect(self._browse_extra)
        g.layout().addWidget(self.cb_merge)
        g.layout().addWidget(_row(self.le_extra, btn_browse))
        lay.addWidget(g)

        g = _grp("Сломанная длительность (метаданные)")
        self.cb_broken = QCheckBox("Включить")
        self.cmb_broken = QComboBox()
        self.cmb_broken.addItems(["Очень большая", "Очень маленькая", "Случайная", "Максимум"])
        g.layout().addWidget(self.cb_broken)
        g.layout().addWidget(_row(QLabel("Тип:"), self.cmb_broken))
        lay.addWidget(g)

        lay.addStretch()

    def _browse_extra(self):
        p, _ = QFileDialog.getOpenFileName(self, "Выберите аудиофайл", "",
            "Аудио (*.mp3 *.wav *.flac *.ogg *.aac)")
        if p:
            self.le_extra.setText(p)

    def get_values(self) -> dict:
        return {
            "trim_silence": self.cb_trim.isChecked(),
            "trim_duration": self.sp_trim.value(),
            "trim_silence_threshold": self.sp_trim_thresh.value(),
            "cut_fragment": self.cb_cut.isChecked(),
            "cut_position_percent": self.sp_cut_pos.value(),
            "cut_duration": self.sp_cut_dur.value(),
            "fade_out": self.cb_fade.isChecked(),
            "fade_duration": self.sp_fade.value(),
            "merge": self.cb_merge.isChecked(),
            "extra_track_path": self.le_extra.text().strip() or None,
            "broken_duration": self.cb_broken.isChecked(),
            "broken_type": self.cmb_broken.currentIndex(),
        }

    def set_values(self, d: dict):
        self.cb_trim.setChecked(d.get("trim_silence", False))
        self.sp_trim.setValue(d.get("trim_duration", 5.0))
        self.sp_trim_thresh.setValue(int(d.get("trim_silence_threshold", -60)))
        self.cb_cut.setChecked(d.get("cut_fragment", False))
        self.sp_cut_pos.setValue(int(d.get("cut_position_percent", 50)))
        self.sp_cut_dur.setValue(d.get("cut_duration", 2.0))
        self.cb_fade.setChecked(d.get("fade_out", False))
        self.sp_fade.setValue(d.get("fade_duration", 5.0))
        self.cb_merge.setChecked(d.get("merge", False))
        self.le_extra.setText(d.get("extra_track_path") or "")
        self.cb_broken.setChecked(d.get("broken_duration", False))
        self.cmb_broken.setCurrentIndex(int(d.get("broken_type", 0)))


class TechnicalTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setSpacing(8)
        lay.setContentsMargins(8, 8, 8, 8)

        g = _grp("Метаданные и теги")
        self.cb_fake_meta = QCheckBox("Фейковые метаданные (случайный comment)")
        self.cb_reorder = QCheckBox("Перемешать порядок ID3-тегов")
        g.layout().addWidget(self.cb_fake_meta)
        g.layout().addWidget(self.cb_reorder)
        lay.addWidget(g)

        g = _grp("Битрейт и кодирование")
        self.cb_bitrate_j = QCheckBox("Джиттер битрейта (случайный из 192/224/256/320)")
        self.cb_frame_sh = QCheckBox("Удалить Xing/Info заголовок")
        g.layout().addWidget(self.cb_bitrate_j)
        g.layout().addWidget(self.cb_frame_sh)
        lay.addWidget(g)

        lay.addStretch()

    def get_values(self) -> dict:
        return {
            "fake_metadata": self.cb_fake_meta.isChecked(),
            "reorder_tags": self.cb_reorder.isChecked(),
            "bitrate_jitter": self.cb_bitrate_j.isChecked(),
            "frame_shift": self.cb_frame_sh.isChecked(),
        }

    def set_values(self, d: dict):
        self.cb_fake_meta.setChecked(d.get("fake_metadata", False))
        self.cb_reorder.setChecked(d.get("reorder_tags", False))
        self.cb_bitrate_j.setChecked(d.get("bitrate_jitter", False))
        self.cb_frame_sh.setChecked(d.get("frame_shift", False))


class SystemTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setSpacing(8)
        lay.setContentsMargins(8, 8, 8, 8)

        g = _grp("Многопоточность")
        self.sp_workers = _int(4, 1, 16)
        self.sp_delay = _dbl(0.0, 0.0, 5.0, 0.1, 1)
        g.layout().addWidget(_row(QLabel("Потоков:"), self.sp_workers, None))
        g.layout().addWidget(_row(QLabel("Задержка между потоками (с):"), self.sp_delay, None))
        lay.addWidget(g)

        g = _grp("Качество промежуточных файлов")
        self.cb_lossless = QCheckBox("Lossless промежуточные файлы (WAV вместо MP3)")
        self.cb_lossless.setChecked(True)
        g.layout().addWidget(self.cb_lossless)
        g.layout().addWidget(QLabel(
            "Рекомендуется: исключает повторное перекодирование при нарезке/склейке",
            styleSheet="color:gray;font-size:9px;"
        ))
        lay.addWidget(g)

        g = _grp("Горячие клавиши")
        shortcuts = [
            ("Ctrl+O", "Добавить файлы"),
            ("Ctrl+R", "Запустить обработку"),
            ("Ctrl+S", "Остановить"),
            ("Delete",  "Удалить выбранные"),
            ("Ctrl+A",  "Выделить все файлы"),
        ]
        for key, desc in shortcuts:
            g.layout().addWidget(QLabel(f"  {key:12s} — {desc}"))
        lay.addWidget(g)

        lay.addStretch()

    def get_values(self) -> dict:
        return {
            "max_workers": self.sp_workers.value(),
            "thread_delay": self.sp_delay.value(),
            "lossless_intermediate": self.cb_lossless.isChecked(),
        }

    def set_values(self, d: dict):
        self.sp_workers.setValue(int(d.get("max_workers", 4)))
        self.sp_delay.setValue(d.get("thread_delay", 0.0))
        self.cb_lossless.setChecked(d.get("lossless_intermediate", True))


class NamesTab(QWidget):
    preset_deleted = pyqtSignal()
    preset_saved   = pyqtSignal()

    _BUILTIN_TEMPLATES = [
        "VK_{n:03d}_custom",
        "{n:03d}. {original}",
        "{original}_{n:03d}",
        "{artist} - {title}",
        "{n:03d}. {artist} - {title}",
        "{n:02d}_{original}",
        "track_{n:04d}",
        "{title} [{year}]",
        "{artist} - {album} - {n:03d}",
        "{n:03d}_{title}_{artist}",
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setSpacing(8)
        lay.setContentsMargins(8, 8, 8, 8)

        g = _grp("Шаблон имени выходного файла")
        self.le_template = QLineEdit("VK_{n:03d}_custom")
        g.layout().addWidget(self.le_template)

        row1 = QWidget()
        rh1 = QHBoxLayout(row1)
        rh1.setContentsMargins(0, 0, 0, 0)
        rh1.setSpacing(4)
        for var in ["{n}", "{n:02d}", "{n:03d}", "{n:04d}", "{original}"]:
            btn = QPushButton(var)
            btn.setFixedWidth(74)
            btn.clicked.connect(lambda checked, v=var: self._insert(v))
            rh1.addWidget(btn)
        rh1.addStretch()
        g.layout().addWidget(row1)

        row2 = QWidget()
        rh2 = QHBoxLayout(row2)
        rh2.setContentsMargins(0, 0, 0, 0)
        rh2.setSpacing(4)
        for var in ["{title}", "{artist}", "{album}", "{year}", "{genre}"]:
            btn = QPushButton(var)
            btn.setFixedWidth(74)
            btn.clicked.connect(lambda checked, v=var: self._insert(v))
            rh2.addWidget(btn)
        rh2.addStretch()
        g.layout().addWidget(row2)

        self.lbl_preview = QLabel("Предпросмотр: VK_001_custom.mp3")
        self.lbl_preview.setStyleSheet("color: #88cc88; font-family: Consolas; font-size: 10px;")
        g.layout().addWidget(self.lbl_preview)
        lay.addWidget(g)

        g_tpl = _grp("Шаблоны имён")
        self.cmb_builtin = QComboBox()
        self.cmb_builtin.addItems(self._BUILTIN_TEMPLATES)
        btn_apply_builtin = QPushButton("Применить")
        btn_apply_builtin.setFixedWidth(90)
        btn_apply_builtin.clicked.connect(self._apply_builtin)
        g_tpl.layout().addWidget(_row(QLabel("Встроенные:"), self.cmb_builtin, btn_apply_builtin))

        self.lst_user_tpl = QListWidget()
        self.lst_user_tpl.setFixedHeight(90)
        self.lst_user_tpl.itemDoubleClicked.connect(self._apply_user_tpl)
        g_tpl.layout().addWidget(QLabel("Пользовательские:"))
        g_tpl.layout().addWidget(self.lst_user_tpl)
        g_tpl.layout().addWidget(_row(
            QPushButton("Сохранить текущий", clicked=self._save_user_tpl),
            QPushButton("Применить", clicked=self._apply_user_tpl),
            QPushButton("Удалить", clicked=self._del_user_tpl),
        ))
        lay.addWidget(g_tpl)

        g2 = _grp("Сохранённые пресеты настроек")
        self.lst_presets = QListWidget()
        self.lst_presets.setFixedHeight(100)
        g2.layout().addWidget(self.lst_presets)
        g2.layout().addWidget(_row(
            QPushButton("Сохранить текущие настройки", clicked=self._save_preset_signal),
            QPushButton("Загрузить", clicked=self._load_preset_signal),
            QPushButton("Удалить", clicked=self._del_preset),
        ))
        lay.addWidget(g2)

        g_meta = _grp("Принудительные метаданные для всех треков")
        g_meta.setToolTip("Если поле заполнено — оно перезаписывает тег у ВСЕХ обрабатываемых треков")
        meta_labels = ["Название", "Исполнитель", "Альбом", "Год", "Жанр", "Комментарий"]
        self._meta_override_fields: dict[str, QLineEdit] = {}
        self._meta_append_checks: dict[str, QCheckBox] = {}
        meta_grid = QGridLayout()
        meta_grid.setSpacing(4)
        for i, lbl in enumerate(meta_labels):
            meta_grid.addWidget(QLabel(lbl), i, 0)
            e = QLineEdit()
            e.setPlaceholderText("(оставьте пустым = взять из оригинала)")
            meta_grid.addWidget(e, i, 1)
            self._meta_override_fields[lbl] = e
            cb = QCheckBox("добавить к тегу")
            cb.setToolTip("Текст добавляется к исходному тегу, а не заменяет его")
            meta_grid.addWidget(cb, i, 2)
            self._meta_append_checks[lbl] = cb
        meta_grid.setColumnStretch(1, 1)
        g_meta_inner = QWidget()
        g_meta_inner.setLayout(meta_grid)
        g_meta.layout().addWidget(g_meta_inner)
        g_meta.layout().addWidget(
            QPushButton("Очистить всё", clicked=self._clear_meta_overrides)
        )
        lay.addWidget(g_meta)

        self._presets: list[dict] = []
        self._user_templates: list[str] = []
        self.le_template.textChanged.connect(self._update_preview)
        self._save_cb = None
        self._load_cb = None
        lay.addStretch()

    def _insert(self, var: str):
        self.le_template.insert(var)

    def _apply_builtin(self):
        self.le_template.setText(self.cmb_builtin.currentText())

    def _save_user_tpl(self):
        tpl = self.le_template.text().strip()
        if tpl and tpl not in self._user_templates:
            self._user_templates.append(tpl)
            self.lst_user_tpl.addItem(tpl)
            self._rebuild_template_dropdown()

    def _apply_user_tpl(self, item=None):
        if item is None:
            row = self.lst_user_tpl.currentRow()
            if 0 <= row < len(self._user_templates):
                self.le_template.setText(self._user_templates[row])
        else:
            self.le_template.setText(item.text())

    def _del_user_tpl(self):
        row = self.lst_user_tpl.currentRow()
        if 0 <= row < len(self._user_templates):
            self._user_templates.pop(row)
            self.lst_user_tpl.takeItem(row)
            self._rebuild_template_dropdown()

    def _update_preview(self):
        tpl = self.le_template.text() or "VK_{n:03d}_custom"
        try:
            name = tpl.format(n=1, original="example", title="My Song",
                              artist="Artist", album="Album", year="2024", genre="Genre")
            self.lbl_preview.setText(f"Предпросмотр: {name}.mp3")
            self.lbl_preview.setStyleSheet("color:#88cc88;font-family:Consolas;font-size:10px;")
        except Exception as e:
            self.lbl_preview.setText(f"Ошибка: {e}")
            self.lbl_preview.setStyleSheet("color:#cc4444;font-family:Consolas;font-size:10px;")

    def _save_preset_signal(self):
        if self._save_cb:
            self._save_cb()

    def _load_preset_signal(self):
        if self._load_cb:
            self._load_cb()

    def _del_preset(self):
        row = self.lst_presets.currentRow()
        if 0 <= row < len(self._presets):
            self._presets.pop(row)
            self.lst_presets.takeItem(row)
            self.preset_deleted.emit()

    def refresh_presets(self, presets: list[dict]):
        self._presets = presets
        self.lst_presets.clear()
        for p in presets:
            self.lst_presets.addItem(p.get("name", "Без имени"))

    def refresh_user_templates(self, templates: list[str]):
        self._user_templates = list(templates)
        self.lst_user_tpl.clear()
        for t in self._user_templates:
            self.lst_user_tpl.addItem(t)
        self._rebuild_template_dropdown()

    def _rebuild_template_dropdown(self):
        current = self.cmb_builtin.currentText()
        self.cmb_builtin.clear()
        self.cmb_builtin.addItems(self._BUILTIN_TEMPLATES)
        if self._user_templates:
            self.cmb_builtin.insertSeparator(len(self._BUILTIN_TEMPLATES))
            self.cmb_builtin.addItems(self._user_templates)
        idx = self.cmb_builtin.findText(current)
        if idx >= 0:
            self.cmb_builtin.setCurrentIndex(idx)

    def get_selected_preset(self) -> "dict | None":
        row = self.lst_presets.currentRow()
        if 0 <= row < len(self._presets):
            return self._presets[row]
        return None

    def _clear_meta_overrides(self):
        for e in self._meta_override_fields.values():
            e.clear()
        for cb in self._meta_append_checks.values():
            cb.setChecked(False)

    def get_meta_overrides(self) -> dict:
        lbl_key = {"Название": "title", "Исполнитель": "artist", "Альбом": "album",
                   "Год": "year", "Жанр": "genre", "Комментарий": "comment"}
        result = {key: self._meta_override_fields[lbl].text() for lbl, key in lbl_key.items()}
        for lbl, key in lbl_key.items():
            result[f"_append_{key}"] = self._meta_append_checks[lbl].isChecked()
        return result

    def get_values(self) -> dict:
        return {"filename_template": self.le_template.text() or "VK_{n:03d}_custom",
                **self.get_meta_overrides()}

    def set_values(self, d: dict):
        self.le_template.setText(d.get("filename_template", "VK_{n:03d}_custom"))
        lbl_key = {"Название": "title", "Исполнитель": "artist", "Альбом": "album",
                   "Год": "year", "Жанр": "genre", "Комментарий": "comment"}
        for lbl, key in lbl_key.items():
            self._meta_override_fields[lbl].setText(d.get(key, ""))
            self._meta_append_checks[lbl].setChecked(d.get(f"_append_{key}", False))


class WaveformViewer(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(4, 4, 4, 4)
        lay.setSpacing(4)

        self._pg_available = False
        self._fill_before   = None
        self._fill_after    = None
        self._watermark_item = None
        self._rms_before: float | None = None
        self._rms_after:  float | None = None
        self._anim_phase  = 0.0
        self._loader_before: "_WaveformLoader | None" = None
        self._loader_after:  "_WaveformLoader | None" = None
        try:
            import pyqtgraph as pg
            self._pg = pg
            self.plot = pg.PlotWidget()
            self.plot.setBackground("#1a1a1a")
            self.plot.showGrid(x=True, y=True, alpha=0.15)
            self.plot.setLabel("left", "Амплитуда")
            self.plot.setLabel("bottom", "Время", units="с")
            self.plot.setMinimumHeight(180)
            self._top_before  = self.plot.plot(pen=pg.mkPen("#4488ff", width=1))
            self._bot_before  = self.plot.plot(pen=pg.mkPen("#4488ff", width=1))
            self._top_after   = self.plot.plot(pen=pg.mkPen("#ff8844", width=1))
            self._bot_after   = self.plot.plot(pen=pg.mkPen("#ff8844", width=1))
            lay.addWidget(self.plot)
            self._pg_available = True
        except ImportError:
            lbl = QLabel("(pyqtgraph не установлен — визуализатор недоступен)")
            lbl.setStyleSheet("color:gray;font-size:9px;")
            lay.addWidget(lbl)

        self._anim_timer = QTimer(self)
        self._anim_timer.setInterval(50)
        self._anim_timer.timeout.connect(self._anim_tick)

        info_row = QWidget()
        ih = QHBoxLayout(info_row)
        ih.setContentsMargins(0, 0, 0, 0)
        self.lbl_before = QLabel("До: —")
        self.lbl_after  = QLabel("После: —")
        self.lbl_delta  = QLabel("Δ: —")
        for lbl in (self.lbl_before, self.lbl_after, self.lbl_delta):
            lbl.setStyleSheet("font-size:9px;color:#aaa;")
            ih.addWidget(lbl)
        ih.addStretch()

        lay.addWidget(info_row)

    def show_before(self, filepath: str):
        if self._loader_before and self._loader_before.isRunning():
            self._loader_before.quit()
        self._loader_before = _WaveformLoader(filepath, "before", self)
        self._loader_before.loaded.connect(self._on_waveform_loaded)
        self._loader_before.start()

    def show_after(self, filepath: str):
        if self._loader_after and self._loader_after.isRunning():
            self._loader_after.quit()
        self._loader_after = _WaveformLoader(filepath, "after", self)
        self._loader_after.loaded.connect(self._on_waveform_loaded)
        self._loader_after.start()

    def _on_waveform_loaded(self, which: str, tops, bots, duration: float, rms_db: float, peak_db: float):
        try:
            import numpy as np
            pg = self._pg if self._pg_available else None
            info = f"RMS {rms_db:.1f} dBFS  Peak {peak_db:.1f} dBFS"
            t = np.linspace(0, duration, len(tops))

            if which == "before":
                self._rms_before = rms_db
                if pg:
                    self._top_before.setData(t, tops)
                    self._bot_before.setData(t, bots)
                    if self._fill_before:
                        self.plot.removeItem(self._fill_before)
                    self._fill_before = pg.FillBetweenItem(
                        self._top_before, self._bot_before,
                        brush=pg.mkBrush(68, 136, 255, 70)
                    )
                    self.plot.addItem(self._fill_before)
                    self.plot.setYRange(-1.15, 1.15, padding=0)
                self.lbl_before.setText(f"До: {info}")
            else:
                self._rms_after = rms_db
                if pg:
                    self._top_after.setData(t, tops)
                    self._bot_after.setData(t, bots)
                    if self._fill_after:
                        self.plot.removeItem(self._fill_after)
                    self._fill_after = pg.FillBetweenItem(
                        self._top_after, self._bot_after,
                        brush=pg.mkBrush(255, 120, 40, 70)
                    )
                    self.plot.addItem(self._fill_after)
                    self.plot.setYRange(-1.15, 1.15, padding=0)
                self.lbl_after.setText(f"После: {info}")

            if self._rms_before is not None and self._rms_after is not None:
                self.lbl_delta.setText(f"Δ RMS: {self._rms_after - self._rms_before:+.1f} dB")
        except Exception:
            pass

    def set_loading(self, loading: bool):
        if loading:
            self.lbl_after.setText("Обработка…")
            if self._pg_available:
                self._top_after.setData([], [])
                self._bot_after.setData([], [])
                if self._fill_after:
                    self.plot.removeItem(self._fill_after)
                self._fill_after = self._pg.FillBetweenItem(
                    self._top_after, self._bot_after,
                    brush=self._pg.mkBrush(255, 120, 40, 35),
                )
                self.plot.addItem(self._fill_after)
                self._anim_phase = 0.0
                self._anim_timer.start()
        else:
            self._anim_timer.stop()
            if self._pg_available:
                self._top_after.setData([], [])
                self._bot_after.setData([], [])
                if self._fill_after:
                    self.plot.removeItem(self._fill_after)
                    self._fill_after = None
                if self._watermark_item:
                    self.plot.removeItem(self._watermark_item)
                    self._watermark_item = None
            if self.lbl_after.text() == "Обработка…":
                self.lbl_after.setText("После: —")

    def _anim_tick(self):
        if not self._pg_available:
            return
        try:
            import numpy as np
            self._anim_phase += 0.07
            t = np.linspace(0, 140, 500)
            # частоты / 140 → та же визуальная плотность циклов на экране
            w = (0.50 * np.sin(2 * np.pi * 0.015  * t + self._anim_phase) +
                 0.28 * np.sin(2 * np.pi * 0.0379 * t + self._anim_phase * 1.5 + 1.0) +
                 0.16 * np.sin(2 * np.pi * 0.0836 * t + self._anim_phase * 0.8 + 2.1) +
                 0.08 * np.sin(2 * np.pi * 0.1357 * t + self._anim_phase * 2.1 + 0.5))
            env = 0.6 + 0.3 * np.sin(self._anim_phase * 0.2)
            w = w * env
            self._top_after.setData(t, w)
            self._bot_after.setData(t, -w[::-1] * 0.75)
            self.plot.setXRange(0, 140, padding=0)
            self.plot.setYRange(-1.15, 1.15, padding=0)
        except Exception:
            pass

    def _clear_after(self):
        self._anim_timer.stop()
        if self._pg_available:
            self._top_after.setData([], [])
            self._bot_after.setData([], [])
            self._top_after.setPen(self._pg.mkPen("#ff8844", width=1))
            if self._fill_after:
                self.plot.removeItem(self._fill_after)
                self._fill_after = None
            if self._watermark_item:
                self.plot.removeItem(self._watermark_item)
                self._watermark_item = None
        self._rms_after = None
        self.lbl_after.setText("После: —")
        self.lbl_delta.setText("Δ: —")

    def show_watermark(self):
        if not self._pg_available:
            self.lbl_after.setText("После: готово")
            return
        self._anim_timer.stop()
        self._clear_after()
        try:
            import numpy as np
            from PyQt6.QtGui import QPainterPath
            from PyQt6.QtCore import QPointF

            text = "vk.com/reuploadunder"
            font = QFont("Consolas", 64, QFont.Weight.Bold)

            # Геометрический контур шрифта (bezier-кривые) → набор полигонов
            path = QPainterPath()
            path.addText(QPointF(0.0, 0.0), font, text)
            rect = path.boundingRect()
            if rect.width() < 1 or rect.height() < 1:
                raise ValueError("empty path")

            polygons = path.toSubpathPolygons()

            xs_list: list[float] = []
            ys_list: list[float] = []
            for poly in polygons:
                n = poly.size()
                if n < 2:
                    continue
                for i in range(n):
                    pt = poly.at(i)
                    xs_list.append(pt.x())
                    ys_list.append(pt.y())
                # замыкаем контур
                pt0 = poly.at(0)
                xs_list.append(pt0.x())
                ys_list.append(pt0.y())
                # разрыв между подпутями
                xs_list.append(float('nan'))
                ys_list.append(float('nan'))

            if not xs_list:
                raise ValueError("no polygons")

            xs_arr = np.array(xs_list, dtype=float)
            ys_arr = np.array(ys_list, dtype=float)

            # Нормализация X в 0..140
            x_min, x_max = rect.left(), rect.right()
            xs_norm = (xs_arr - x_min) / (x_max - x_min) * 140

            # Нормализация Y: Qt-координаты вниз, поэтому инвертируем
            y_min, y_max = rect.top(), rect.bottom()
            y_center = (y_min + y_max) / 2
            y_scale = 1.7 / (y_max - y_min)
            ys_norm = -(ys_arr - y_center) * y_scale

            # Рисуем все контуры линиями, NaN → разрыв
            self._top_after.setPen(self._pg.mkPen("#ff8844", width=2))
            self._top_after.setData(xs_norm, ys_norm, connect='finite')
            self._bot_after.setData([], [])

            self.plot.setXRange(0, 140, padding=0.02)
            self.plot.setYRange(-1.15, 1.15, padding=0)
            self.lbl_after.setText("После: —")

        except Exception:
            self._watermark_item = self._pg.TextItem(
                html='<span style="color:#666; font-family:Consolas; font-size:13pt; font-weight:bold;">vk.com/reuploadunder</span>',
                anchor=(0.5, 0.5),
            )
            self.plot.addItem(self._watermark_item)
            self._watermark_item.setPos(70, 0)
            self.plot.setXRange(0, 140, padding=0.02)
            self.plot.setYRange(-1.15, 1.15, padding=0)
            self.lbl_after.setText("После: —")


class LogPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        self.txt = QTextEdit()
        self.txt.setReadOnly(True)
        self.txt.setFont(QFont("Consolas", 9))
        self.txt.setMaximumHeight(180)
        self.txt.document().setMaximumBlockCount(1000)
        lay.addWidget(self.txt)

    def append(self, msg: str, level: str = "info"):
        colors = {"info": "#cccccc", "success": "#55cc55",
                  "warning": "#ffaa44", "error": "#ff5555"}
        color = colors.get(level, "#cccccc")
        ts = datetime.now().strftime("%H:%M:%S")
        html = (f'<span style="color:#888">[{ts}]</span> '
                f'<span style="color:{color}">{msg}</span><br>')
        self.txt.moveCursor(self.txt.textCursor().MoveOperation.End)
        self.txt.insertHtml(html)
        self.txt.moveCursor(self.txt.textCursor().MoveOperation.End)

    def clear(self):
        self.txt.clear()


class ModifierPanel(QWidget):
    start_requested   = pyqtSignal()
    stop_requested    = pyqtSignal()
    preview_requested = pyqtSignal(str, object, dict, dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        outer.addWidget(scroll, 1)

        content = QWidget()
        scroll.setWidget(content)
        lay = QVBoxLayout(content)
        lay.setSpacing(6)
        lay.setContentsMargins(6, 6, 6, 6)

        top_row = QWidget()
        th = QHBoxLayout(top_row)
        th.setContentsMargins(0, 0, 0, 0)
        th.setSpacing(6)
        self.cover = CoverWidget()
        self.metadata = MetadataWidget()
        self.track_info = TrackInfoWidget()
        th.addWidget(self.cover)
        th.addWidget(self.metadata, 1)
        th.addWidget(self.track_info)
        lay.addWidget(top_row)

        lay.addWidget(_hline())

        self.tabs = QTabWidget()
        self.basic_tab    = BasicTab()
        self.spectral_tab = SpectralTab()
        self.texture_tab  = TextureTab()
        self.advanced_tab = AdvancedTab()
        self.technical_tab = TechnicalTab()
        self.system_tab   = SystemTab()
        self.names_tab    = NamesTab()
        self.tabs.addTab(self.basic_tab,    "Базовые")
        self.tabs.addTab(self.spectral_tab, "Спектральные")
        self.tabs.addTab(self.texture_tab,  "Текстурные")
        self.tabs.addTab(self.advanced_tab, "Продвинутые")
        self.tabs.addTab(self.technical_tab,"Технические")
        self.tabs.addTab(self.system_tab,   "Системные")
        self.tabs.addTab(self.names_tab,    "Имена")
        lay.addWidget(self.tabs)

        out_g = _grp("Настройки вывода")
        out_lay = QGridLayout()
        out_g.layout().addLayout(out_lay)

        self.lbl_out_dir = QLabel(os.path.expanduser("~/Desktop/Output"))
        self.lbl_out_dir.setStyleSheet("border:1px solid #555;padding:2px;")
        self.lbl_out_dir.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        out_lay.addWidget(QPushButton("Выбрать папку", clicked=self._select_out_dir), 0, 0)
        out_lay.addWidget(self.lbl_out_dir, 0, 1)
        out_lay.setColumnStretch(1, 1)

        self.cb_preserve_meta  = QCheckBox("Сохранять оригинальные теги")
        self.cb_preserve_cover = QCheckBox("Сохранять оригинальную обложку")
        self.cb_rename         = QCheckBox("Рандомные названия в тегах")
        self.cb_delete_orig    = QCheckBox("Удалять оригиналы после обработки")
        self.cb_preserve_meta.setChecked(True)
        self.cb_preserve_cover.setChecked(True)
        self.cb_rename.setChecked(True)

        self.cmb_quality = QComboBox()
        self.cmb_quality.addItems(QUALITY_PRESETS["mp3"])
        self.cmb_quality.setCurrentText("320 kbps (CBR)")

        out_lay.addWidget(self.cb_preserve_meta,  1, 0, 1, 2)
        out_lay.addWidget(self.cb_preserve_cover, 2, 0, 1, 2)
        out_lay.addWidget(self.cb_rename,         3, 0, 1, 2)
        out_lay.addWidget(self.cb_delete_orig,    4, 0, 1, 2)
        out_lay.addWidget(QLabel("Качество:"),    5, 0)
        out_lay.addWidget(self.cmb_quality,       5, 1)
        lay.addWidget(out_g)

        self.waveform = WaveformViewer()
        self.waveform.setMaximumHeight(220)
        outer.addWidget(self.waveform)

        action_w = QWidget()
        ah = QHBoxLayout(action_w)
        ah.setContentsMargins(6, 4, 6, 4)
        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        self.btn_start = QPushButton("▶  Запустить обработку")
        self.btn_start.setFixedWidth(180)
        self.btn_start.clicked.connect(self.start_requested)
        self.btn_stop  = QPushButton("■  Стоп")
        self.btn_stop.setFixedWidth(80)
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_requested)
        self.btn_open  = QPushButton("Открыть папку")
        self.btn_open.clicked.connect(self._open_out_dir)
        self.lbl_eta = QLabel("")
        self.lbl_eta.setStyleSheet("color:gray;font-size:9px;")
        ah.addWidget(self.progress, 1)
        ah.addWidget(self.lbl_eta)
        ah.addStretch()
        ah.addWidget(self.btn_open)
        ah.addWidget(self.btn_stop)
        ah.addWidget(self.btn_start)
        outer.addWidget(action_w)

        self.log = LogPanel()
        outer.addWidget(self.log)

        self._output_dir = os.path.expanduser("~/Desktop/Output")
        self._presets: list[dict] = []
        self._process_start = 0.0
        self._completed = 0
        self._total = 0
        self._current_filepath: str | None = None
        self._current_track = None

        self.names_tab._save_cb = self._save_preset
        self.names_tab._load_cb = self._load_preset

        self._preview_timer = QTimer(self)
        self._preview_timer.setSingleShot(True)
        self._preview_timer.setInterval(0)
        self._preview_timer.timeout.connect(self._on_preview_requested)
        self._connect_settings_signals(self.tabs)

    def _select_out_dir(self):
        d = QFileDialog.getExistingDirectory(self, "Выберите папку для сохранения")
        if d:
            self._output_dir = d
            self.lbl_out_dir.setText(d)

    def _open_out_dir(self):
        d = self._output_dir
        if os.path.isdir(d):
            if sys.platform == "win32":
                os.startfile(d)
            else:
                subprocess.Popen(["xdg-open", d])

    def _save_preset(self):
        from PyQt6.QtWidgets import QInputDialog
        name, ok = QInputDialog.getText(self, "Сохранить пресет", "Имя пресета:")
        if ok and name:
            cfg = self.collect_all_settings()
            cfg["name"] = name
            self._presets.append(cfg)
            self.names_tab.refresh_presets(self._presets)
            self.names_tab.preset_saved.emit()

    def _load_preset(self):
        p = self.names_tab.get_selected_preset()
        if p:
            self.restore_all_settings(p)

    def _del_preset(self):
        row = self.names_tab.lst_presets.currentRow()
        if 0 <= row < len(self._presets):
            self._presets.pop(row)
            self.names_tab.refresh_presets(self._presets)
            self.names_tab.preset_deleted.emit()

    def _connect_settings_signals(self, root: QWidget):
        preview_tabs = (self.basic_tab, self.spectral_tab, self.texture_tab, self.advanced_tab)
        for tab in preview_tabs:
            for w in tab.findChildren(QCheckBox):
                w.stateChanged.connect(self._schedule_preview)
            for w in tab.findChildren(QSpinBox):
                w.valueChanged.connect(self._schedule_preview)
            for w in tab.findChildren(QDoubleSpinBox):
                w.valueChanged.connect(self._schedule_preview)
            for w in tab.findChildren(QComboBox):
                w.currentIndexChanged.connect(self._schedule_preview)

    def _schedule_preview(self):
        if self._current_filepath:
            self._preview_timer.start()

    def _on_preview_requested(self):
        if not self._current_filepath:
            return
        settings  = self.collect_all_settings()
        metadata  = self.names_tab.get_meta_overrides()
        self.preview_requested.emit(
            self._current_filepath,
            self._current_track,
            settings,
            metadata,
        )

    def on_file_selected(self, track: "TrackInfo | None", filepath: "str | None"):
        self._current_filepath = filepath
        self._current_track    = track
        self.track_info.show_track(track)
        self.metadata.set_from_track(track)
        self.cover.set_from_track(track)
        self.waveform._clear_after()
        if filepath and os.path.exists(filepath):
            QTimer.singleShot(100, lambda: self.waveform.show_before(filepath))

    def on_processing_start(self, total: int):
        self._total = total
        self._completed = 0
        self._last_output = None
        self._process_start = time.time()
        self.progress.setMaximum(total)
        self.progress.setValue(0)
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.lbl_eta.setText("")
        self.waveform.set_loading(True)

    def on_progress(self, cur: int, total: int, filepath: str):
        self.log.append(f"[{cur}/{total}] {os.path.basename(filepath)}", "info")

    def on_file_done(self, filepath: str, ok: bool, output: str):
        self._completed += 1
        self.progress.setValue(self._completed)
        if ok:
            self.log.append(f"OK  {os.path.basename(filepath)} → {os.path.basename(output)}", "success")
            self._last_output = output
        else:
            self.log.append(f"ERR {os.path.basename(filepath)}", "error")
        elapsed = time.time() - self._process_start
        if self._completed > 0 and self._total > 0:
            eta = elapsed / self._completed * (self._total - self._completed)
            self.lbl_eta.setText(f"ETA: {int(eta // 60)}:{int(eta % 60):02d}")

    def on_all_done(self, success: int, total: int):
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.progress.setValue(total)
        self.lbl_eta.setText("")
        self.log.append(f"Готово: {success}/{total} файлов обработано", "success")
        self.waveform.set_loading(False)
        if self._last_output:
            self._last_output = None
            QTimer.singleShot(300, lambda: self.waveform.show_watermark())

    def on_error(self, msg: str):
        self.log.append(f"ОШИБКА: {msg}", "error")

    def collect_all_settings(self) -> dict:
        b = self.basic_tab.get_values()
        s = self.spectral_tab.get_values()
        t = self.texture_tab.get_values()
        a = self.advanced_tab.get_values()
        tech = self.technical_tab.get_values()
        sys_v = self.system_tab.get_values()
        names = self.names_tab.get_values()
        quality_str = self.cmb_quality.currentText()
        quality = QUALITY_MAP.get(quality_str, quality_str)

        return {
            "methods": {
                "pitch":               b["pitch"],
                "speed":               b["speed"],
                "eq":                  b["eq"],
                "silence":             b["silence"],
                "loudnorm":            b["loudnorm"],
                "phase_invert":        s["phase_invert"],
                "phase_scramble":      s["phase_scramble"],
                "dc_shift":            s["dc_shift"],
                "resample_drift":      s["resample_drift"],
                "haas_delay":          s["haas_delay"],
                "ultrasonic_noise":    s["ultrasonic_noise"],
                "dither_attack":       s["dither_attack"],
                "id3_padding_attack":  s["id3_padding_attack"],
                "spectral_masking":    t["spectral_masking"],
                "concert_emulation":   t["concert_emulation"],
                "midside_processing":  t["midside_processing"],
                "psychoacoustic_noise":t["psychoacoustic_noise"],
                "saturation":          t["saturation"],
                "temporal_jitter":     t["temporal_jitter"],
                "spectral_jitter":     t["spectral_jitter"],
                "vk_infrasonic":       t["vk_infrasonic"],
                "trim_silence":        a["trim_silence"],
                "cut_fragment":        a["cut_fragment"],
                "fade_out":            a["fade_out"],
                "merge":               a["merge"],
                "broken_duration":     a["broken_duration"],
                "bitrate_jitter":      tech["bitrate_jitter"],
                "frame_shift":         tech["frame_shift"],
                "fake_metadata":       tech["fake_metadata"],
                "reorder_tags":        tech["reorder_tags"],
            },
            "pitch_value":            b["pitch_value"],
            "speed_value":            b["speed_value"],
            "eq_type":                b["eq_type"],
            "eq_value":               b["eq_value"],
            "silence_duration":       b["silence_duration"],
            "loudnorm_target":        b["loudnorm_target"],
            "phase_invert_strength":  s["phase_invert_strength"],
            "phase_scramble_speed":   s["phase_scramble_speed"],
            "dc_shift_value":         s["dc_shift_value"],
            "resample_drift_amount":  s["resample_drift_amount"],
            "haas_delay_ms":          s["haas_delay_ms"],
            "ultrasonic_freq":        s["ultrasonic_freq"],
            "ultrasonic_level":       s["ultrasonic_level"],
            "dither_method":          s["dither_method"],
            "id3_padding_bytes":      s["id3_padding_bytes"],
            "spectral_mask_sensitivity":   t["spectral_mask_sensitivity"],
            "spectral_mask_attenuation":   t["spectral_mask_attenuation"],
            "spectral_mask_peaks":         t["spectral_mask_peaks"],
            "concert_intensity":           t["concert_intensity"],
            "midside_mid_gain":            t["midside_mid_gain"],
            "midside_side_gain":           t["midside_side_gain"],
            "psychoacoustic_intensity":    t["psychoacoustic_intensity"],
            "saturation_drive":            t["saturation_drive"],
            "saturation_mix":              t["saturation_mix"],
            "jitter_intensity":            t["jitter_intensity"],
            "jitter_frequency":            t["jitter_frequency"],
            "spectral_jitter_count":       t["spectral_jitter_count"],
            "spectral_jitter_attenuation": t["spectral_jitter_attenuation"],
            "spectral_jitter_fixed_frequencies": t["spectral_jitter_fixed_frequencies"],
            "spectral_jitter_fixed_attenuation": t["spectral_jitter_fixed_attenuation"],
            "spectral_jitter_manual_config":     t["spectral_jitter_manual_config"],
            "_sj_rows":                          t["_sj_rows"],
            "vk_infrasonic_mode":          t["vk_infrasonic_mode"],
            "vk_infrasonic_freq":          t["vk_infrasonic_freq"],
            "vk_infrasonic_amplitude":     t["vk_infrasonic_amplitude"],
            "vk_infrasonic_mod_freq":      t["vk_infrasonic_mod_freq"],
            "vk_infrasonic_mod_depth":     t["vk_infrasonic_mod_depth"],
            "vk_infrasonic_phase_shift":   t["vk_infrasonic_phase_shift"],
            "vk_infrasonic_waveform":      t["vk_infrasonic_waveform"],
            "vk_infrasonic_adaptive_amplitude": t["vk_infrasonic_adaptive_amplitude"],
            "vk_infrasonic_harmonics":     t["vk_infrasonic_harmonics"],
            "trim_duration":          a["trim_duration"],
            "trim_silence_threshold": a["trim_silence_threshold"],
            "cut_position_percent":   a["cut_position_percent"],
            "cut_duration":           a["cut_duration"],
            "fade_duration":          a["fade_duration"],
            "extra_track_path":       a["extra_track_path"],
            "broken_type":            a["broken_type"],
            "lossless_intermediate":  sys_v["lossless_intermediate"],
            "max_workers":            sys_v["max_workers"],
            "thread_delay":           sys_v["thread_delay"],
            "filename_template":      names["filename_template"],
            "quality":                quality,
            "rename_files":           self.cb_rename.isChecked(),
            "preserve_metadata":      self.cb_preserve_meta.isChecked(),
            "preserve_cover":         self.cb_preserve_cover.isChecked(),
            "selected_cover_path":    self.cover.get_path(),
            "delete_original":        self.cb_delete_orig.isChecked(),
            "_quality_str":           quality_str,
            "_output_dir":            self._output_dir,
            "_meta_locks":            self.metadata.get_lock_states(),
        }

    def restore_all_settings(self, d: dict):
        self.basic_tab.set_values(d)
        self.spectral_tab.set_values(d)
        self.texture_tab.set_values(d)
        self.advanced_tab.set_values(d)
        self.technical_tab.set_values(d)
        self.system_tab.set_values(d)
        self.names_tab.set_values(d)
        q = d.get("_quality_str", d.get("quality", "320 kbps (CBR)"))
        idx = self.cmb_quality.findText(q)
        if idx < 0:
            idx = 0
        self.cmb_quality.setCurrentIndex(idx)
        self.cb_rename.setChecked(d.get("rename_files", True))
        self.cb_preserve_meta.setChecked(d.get("preserve_metadata", True))
        self.cb_preserve_cover.setChecked(d.get("preserve_cover", True))
        self.cb_delete_orig.setChecked(d.get("delete_original", False))
        if d.get("_output_dir"):
            self._output_dir = d["_output_dir"]
            self.lbl_out_dir.setText(self._output_dir)
        self.metadata.set_values(d)


class ConverterPanel(QWidget):
    start_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.setSpacing(8)

        hdr = QLabel("Конвертер аудиоформатов")
        hdr.setFont(QFont("", 13, QFont.Weight.Bold))
        lay.addWidget(hdr)

        g = _grp("Настройки конвертации")
        self.cmb_fmt = QComboBox()
        self.cmb_fmt.addItems(list(SUPPORTED_FORMATS.keys()))
        self.cmb_quality = QComboBox()
        self.lbl_fmt_desc = QLabel("")
        self.lbl_fmt_desc.setStyleSheet("color:gray;font-size:9px;")
        g.layout().addWidget(_row(QLabel("Формат:"), self.cmb_fmt, self.lbl_fmt_desc, None))
        g.layout().addWidget(_row(QLabel("Качество:"), self.cmb_quality, None))
        lay.addWidget(g)

        g2 = _grp("Вывод")
        self._out_dir = os.path.expanduser("~/Desktop/Output")
        self.lbl_out = QLabel(self._out_dir)
        self.lbl_out.setStyleSheet("border:1px solid #555;padding:2px;")
        self.cb_del_orig = QCheckBox("Удалять оригиналы")
        g2.layout().addWidget(_row(QPushButton("Выбрать папку", clicked=self._select_dir), self.lbl_out))
        g2.layout().addWidget(self.cb_del_orig)
        lay.addWidget(g2)

        action_w = QWidget()
        ah = QHBoxLayout(action_w)
        ah.setContentsMargins(0, 0, 0, 0)
        self.progress = QProgressBar()
        self.btn_conv = QPushButton("▶  Запустить конвертацию")
        self.btn_conv.clicked.connect(self.start_requested)
        ah.addWidget(self.progress, 1)
        ah.addWidget(self.btn_conv)
        lay.addWidget(action_w)

        self.log = LogPanel()
        self.log.txt.setMaximumHeight(300)
        lay.addWidget(self.log, 1)

        self.cmb_fmt.currentTextChanged.connect(self._on_fmt_changed)
        self._on_fmt_changed(self.cmb_fmt.currentText())

    def _select_dir(self):
        d = QFileDialog.getExistingDirectory(self, "Выберите папку")
        if d:
            self._out_dir = d
            self.lbl_out.setText(d)

    def _on_fmt_changed(self, fmt: str):
        self.lbl_fmt_desc.setText(SUPPORTED_FORMATS.get(fmt, ""))
        presets = QUALITY_PRESETS.get(fmt)
        self.cmb_quality.clear()
        if presets:
            self.cmb_quality.addItems(presets)
            self.cmb_quality.setEnabled(True)
        else:
            self.cmb_quality.addItem("По умолчанию")
            self.cmb_quality.setEnabled(False)

    def get_params(self) -> dict:
        return {
            "output_format": self.cmb_fmt.currentText(),
            "quality_preset": self.cmb_quality.currentText(),
            "output_dir": self._out_dir,
            "delete_originals": self.cb_del_orig.isChecked(),
        }

    def on_processing_start(self, total: int):
        self.progress.setMaximum(total)
        self.progress.setValue(0)
        self.btn_conv.setEnabled(False)

    def on_progress(self, cur, total, fp):
        self.log.append(f"[{cur}/{total}] {os.path.basename(fp)}", "info")

    def on_file_done(self, fp, ok, out):
        self.progress.setValue(self.progress.value() + 1)
        if ok:
            self.log.append(f"OK  {os.path.basename(fp)} → {os.path.basename(out)}", "success")
        else:
            self.log.append(f"ERR {os.path.basename(fp)}", "error")

    def on_all_done(self, success, total):
        self.btn_conv.setEnabled(True)
        self.log.append(f"Готово: {success}/{total}", "success")

    def on_error(self, msg):
        self.log.append(f"ОШИБКА: {msg}", "error")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("VK Modifier")
        self.resize(1280, 900)
        self.setMinimumSize(1000, 700)

        self._worker: "WorkerThread | None" = None
        self._conv_worker: "ConverterThread | None" = None
        self._preview_worker: "PreviewThread | None" = None
        self._stop_event = threading.Event()
        self._ffmpeg_ok = False

        _appdata = QStandardPaths.writableLocation(QStandardPaths.StandardLocation.AppDataLocation)
        self._config_path = os.path.join(_appdata, "config.json")

        self._tray: "QSystemTrayIcon | None" = None
        if QSystemTrayIcon.isSystemTrayAvailable():
            px = QPixmap(16, 16)
            px.fill(QColor(86, 156, 214))
            self._tray = QSystemTrayIcon(QIcon(px), self)
            self._tray.show()

        self._build_ui()
        self._setup_shortcuts()
        self._load_config()
        self._check_ffmpeg()

    def _build_ui(self):
        tb = self.addToolBar("Режим")
        tb.setMovable(False)
        self._act_modifier  = QAction("Модификатор", self, checkable=True, checked=True)
        self._act_converter = QAction("Конвертер",   self, checkable=True, checked=False)
        self._act_modifier.triggered.connect(lambda: self._switch_mode("modifier"))
        self._act_converter.triggered.connect(lambda: self._switch_mode("converter"))
        tb.addAction(self._act_modifier)
        tb.addAction(self._act_converter)
        tb.addSeparator()
        self._lbl_ffmpeg = QLabel("  FFmpeg: проверка…  ")
        tb.addWidget(self._lbl_ffmpeg)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        self.setCentralWidget(splitter)

        self.file_panel = FileListPanel()
        self.file_panel.setMinimumWidth(260)
        self.file_panel.setMaximumWidth(420)
        self.file_panel.files_changed.connect(self._on_files_changed)
        self.file_panel.lst.itemSelectionChanged.connect(self._on_file_select)
        splitter.addWidget(self.file_panel)

        self._stack = QStackedWidget()
        self.modifier_panel  = ModifierPanel()
        self.converter_panel = ConverterPanel()
        self._stack.addWidget(self.modifier_panel)
        self._stack.addWidget(self.converter_panel)
        splitter.addWidget(self._stack)

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([300, 980])

        self.modifier_panel.start_requested.connect(self._start_modifier)
        self.modifier_panel.stop_requested.connect(self._stop)
        self.modifier_panel.preview_requested.connect(self._start_preview)
        self.converter_panel.start_requested.connect(self._start_converter)
        self.modifier_panel.names_tab.preset_deleted.connect(self._save_config)
        self.modifier_panel.names_tab.preset_saved.connect(self._save_config)

        self._status = self.statusBar()
        self._status.showMessage("Готов")

    def _switch_mode(self, mode: str):
        if mode == "modifier":
            self._stack.setCurrentWidget(self.modifier_panel)
            self._act_modifier.setChecked(True)
            self._act_converter.setChecked(False)
        else:
            self._stack.setCurrentWidget(self.converter_panel)
            self._act_modifier.setChecked(False)
            self._act_converter.setChecked(True)

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+O"), self, self.file_panel._dialog_add)
        QShortcut(QKeySequence("Ctrl+R"), self, self._start_modifier)
        QShortcut(QKeySequence("Ctrl+S"), self, self._stop)
        QShortcut(QKeySequence("Delete"), self, self.file_panel._remove_selected)
        QShortcut(QKeySequence("Ctrl+A"), self, self.file_panel.lst.selectAll)

    def _check_ffmpeg(self):
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, stdin=subprocess.DEVNULL, timeout=5, check=True)
            self._ffmpeg_ok = True
            self._lbl_ffmpeg.setText("  FFmpeg: ✓ найден  ")
            self._lbl_ffmpeg.setStyleSheet("color:#55cc55;")
        except Exception:
            self._ffmpeg_ok = False
            self._lbl_ffmpeg.setText("  FFmpeg: ✗ не найден  ")
            self._lbl_ffmpeg.setStyleSheet("color:#cc4444;")

    def _on_files_changed(self, files: list):
        self._status.showMessage(f"{len(files)} файлов загружено")
        self.file_panel._recent = list(dict.fromkeys(
            self.file_panel._recent + files
        ))[-50:]
        self._save_config()

    def _on_file_select(self):
        track = self.file_panel.current_track()
        fpath = self.file_panel.current_file()
        self.modifier_panel.on_file_selected(track, fpath)


    def _start_modifier(self):
        files = self.file_panel.get_files()
        if not files:
            QMessageBox.warning(self, "Внимание", "Добавьте аудиофайлы.")
            return
        if not self._ffmpeg_ok:
            QMessageBox.critical(self, "Ошибка", "FFmpeg не найден!")
            return

        all_settings = self.modifier_panel.collect_all_settings()

        if all_settings.get("delete_original"):
            ans = QMessageBox.question(
                self, "Подтверждение",
                "Включено удаление оригиналов. Продолжить?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if ans != QMessageBox.StandardButton.Yes:
                return

        out_dir = self.modifier_panel._output_dir
        try:
            os.makedirs(out_dir, exist_ok=True)
            _probe = os.path.join(out_dir, ".vkmod_write_probe")
            with open(_probe, "w") as _f:
                _f.write("")
            os.unlink(_probe)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Папка вывода недоступна для записи:\n{e}")
            return

        metadata = self.modifier_panel.names_tab.get_meta_overrides()
        max_workers = all_settings.get("max_workers", 1)
        delay = all_settings.get("thread_delay", 0.0)

        self._stop_event.clear()
        self.modifier_panel.on_processing_start(len(files))
        self.modifier_panel.log.clear()
        self._save_config()

        self._worker = WorkerThread(
            files=files,
            tracks_info=self.file_panel.get_tracks(),
            output_dir=out_dir,
            settings=all_settings,
            metadata=metadata,
            max_workers=max_workers,
            delay_between=delay,
            stop_event=self._stop_event,
        )
        self._worker.progress.connect(self.modifier_panel.on_progress)
        self._worker.file_done.connect(self._on_modifier_file_done)
        self._worker.all_done.connect(self.modifier_panel.on_all_done)
        self._worker.all_done.connect(self._notify_done)
        self._worker.error_msg.connect(self.modifier_panel.on_error)
        self._worker.start()

    def _on_modifier_file_done(self, filepath: str, ok: bool, output: str):
        self.modifier_panel.on_file_done(filepath, ok, output)
        self.file_panel.color_item(filepath, ok)

    def _stop(self):
        self._stop_event.set()
        self.modifier_panel.btn_stop.setEnabled(False)
        self.modifier_panel.log.append("Остановка запрошена…", "warning")
        if not (self._worker and self._worker.isRunning()):
            self.modifier_panel.btn_start.setEnabled(True)

    def _start_preview(self, filepath: str, track_info, settings: dict, metadata: dict):
        if not self._ffmpeg_ok:
            QMessageBox.critical(self, "Ошибка", "FFmpeg не найден!")
            return
        if self._preview_worker and self._preview_worker.isRunning():
            self._preview_worker.cancel()
            self._preview_worker.wait(3000)
            self._preview_worker.cleanup()

        self.modifier_panel.waveform.set_loading(True)
        self.modifier_panel.log.append(
            f"Предпросмотр: {os.path.basename(filepath)}…", "info"
        )

        self._preview_worker = PreviewThread(
            filepath=filepath,
            track_info=track_info,
            settings=settings,
            metadata=metadata,
        )
        self._preview_worker.done.connect(self._on_preview_done)
        self._preview_worker.error.connect(self._on_preview_error)
        self._preview_worker.start()

    def _on_preview_done(self, output_path: str):
        self.modifier_panel.waveform.set_loading(False)
        self.modifier_panel.waveform.show_after(output_path)
        self.modifier_panel.log.append("Предпросмотр готов.", "success")
        if self._preview_worker:
            QTimer.singleShot(5000, self._preview_worker.cleanup)

    def _on_preview_error(self, msg: str):
        self.modifier_panel.waveform.set_loading(False)
        self.modifier_panel.log.append(f"Предпросмотр: ошибка — {msg}", "error")
        if self._preview_worker:
            QTimer.singleShot(0, self._preview_worker.cleanup)


    def _start_converter(self):
        files = self.file_panel.get_files()
        if not files:
            QMessageBox.warning(self, "Внимание", "Добавьте аудиофайлы.")
            return
        if not self._ffmpeg_ok:
            QMessageBox.critical(self, "Ошибка", "FFmpeg не найден!")
            return

        params = self.converter_panel.get_params()
        os.makedirs(params["output_dir"], exist_ok=True)
        self.converter_panel.on_processing_start(len(files))
        self.converter_panel.log.clear()

        self._conv_worker = ConverterThread(
            files=files,
            output_dir=params["output_dir"],
            output_format=params["output_format"],
            quality_preset=params["quality_preset"],
            max_workers=self.modifier_panel.system_tab.sp_workers.value(),
            delete_originals=params["delete_originals"],
        )
        self._conv_worker.progress.connect(self.converter_panel.on_progress)
        self._conv_worker.file_done.connect(self.converter_panel.on_file_done)
        self._conv_worker.all_done.connect(self.converter_panel.on_all_done)
        self._conv_worker.error_msg.connect(self.converter_panel.on_error)
        self._conv_worker.start()

    def _notify_done(self, success: int, total: int):
        if self._tray and self._tray.isVisible():
            self._tray.showMessage(
                "VK Modifier",
                f"Готово: {success}/{total} файлов обработано",
                QSystemTrayIcon.MessageIcon.Information,
                4000,
            )


    def _load_config(self):
        try:
            if os.path.exists(self._config_path):
                with open(self._config_path, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                self.modifier_panel.restore_all_settings(cfg.get("settings", {}))
                presets = cfg.get("presets", [])
                if presets:
                    self.modifier_panel._presets = presets
                    self.modifier_panel.names_tab.refresh_presets(presets)
                tpl_presets = cfg.get("template_presets", [])
                if tpl_presets:
                    self.modifier_panel.names_tab.refresh_user_templates(tpl_presets)
                self.file_panel._recent = cfg.get("recent_files", [])
        except Exception:
            pass

    def _save_config(self):
        try:
            os.makedirs(os.path.dirname(self._config_path), exist_ok=True)
            settings = self.modifier_panel.collect_all_settings()
            with open(self._config_path, "w", encoding="utf-8") as f:
                json.dump({
                    "settings": settings,
                    "presets": self.modifier_panel._presets,
                    "template_presets": self.modifier_panel.names_tab._user_templates,
                    "recent_files": self.file_panel._recent,
                }, f, indent=2, ensure_ascii=False)
        except Exception as e:
            import traceback; traceback.print_exc()

    def closeEvent(self, event):
        self._save_config()
        self._stop_event.set()
        event.accept()


def _apply_dark_palette(app: QApplication):
    app.setStyle("Fusion")
    pal = QPalette()
    dark  = QColor(45,  45,  48)
    mid   = QColor(63,  63,  70)
    light = QColor(100, 100, 110)
    text  = QColor(220, 220, 220)
    hi    = QColor(86,  156, 214)
    pal.setColor(QPalette.ColorRole.Window,          dark)
    pal.setColor(QPalette.ColorRole.WindowText,      text)
    pal.setColor(QPalette.ColorRole.Base,            QColor(30, 30, 30))
    pal.setColor(QPalette.ColorRole.AlternateBase,   dark)
    pal.setColor(QPalette.ColorRole.ToolTipBase,     dark)
    pal.setColor(QPalette.ColorRole.ToolTipText,     text)
    pal.setColor(QPalette.ColorRole.Text,            text)
    pal.setColor(QPalette.ColorRole.Button,          mid)
    pal.setColor(QPalette.ColorRole.ButtonText,      text)
    pal.setColor(QPalette.ColorRole.BrightText,      QColor(255, 100, 100))
    pal.setColor(QPalette.ColorRole.Highlight,       hi)
    pal.setColor(QPalette.ColorRole.HighlightedText, QColor(0, 0, 0))
    pal.setColor(QPalette.ColorRole.Link,            hi)
    pal.setColor(QPalette.ColorRole.Mid,             light)
    app.setPalette(pal)


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("VKModifier")
    _apply_dark_palette(app)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
