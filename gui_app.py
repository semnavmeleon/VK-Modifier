import sys
import os
import random
import json
import subprocess
import tempfile
import queue
import threading
import math
import struct
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from core_logic import TrackInfo, ModificationWorker

try:
    from tkinterdnd2 import DND_FILES as _DND_FILES
    _DND_AVAILABLE = True
except ImportError:
    _DND_AVAILABLE = False
    _DND_FILES = None

CONFIG_FILE = "vk_modifier_config.json"
SUPPORTED_FORMATS = {
    'mp3': 'MP3 (lossy)',
    'flac': 'FLAC (lossless)',
    'wav': 'WAV (lossless)',
    'ogg': 'OGG Vorbis (lossy)',
    'aac': 'AAC (lossy)',
    'm4a': 'M4A AAC (lossy)',
    'wma': 'WMA (lossy)',
    'opus': 'Opus (lossy)',
    'aiff': 'AIFF (lossless)',
    'alac': 'ALAC (lossless)',
    'wv': 'WavPack (lossless/hybrid)',
    'ape': "Monkey's Audio (lossless)",
    'tta': 'True Audio (lossless)',
    'ac3': 'AC3/Dolby Digital (lossy)',
    'dts': 'DTS (lossy)',
    'mp2': 'MPEG Layer 2 (lossy)',
    'mpc': 'Musepack (lossy)',
    'spx': 'Speex (speech)',
    'amr': 'AMR (speech)',
    'au': 'AU/Sun Audio (uncompressed)',
    'mka': 'Matroska Audio (container)',
    'oga': 'Ogg FLAC (lossless)',
    'caf': 'Core Audio Format (uncompressed)',
    'shn': 'Shorten (lossless)',
}

INPUT_EXTENSIONS = [
    ("Все аудио файлы", ".mp3 .flac .wav .ogg .aac .m4a .wma .opus .aiff .alac .wv .ape .tta .ac3 .dts .mp2 .mpc .spx .amr .au .mka .oga .caf .shn"),
    ("MP3 files", ".mp3"),
    ("FLAC files", ".flac"),
    ("WAV files", ".wav"),
    ("OGG files", ".ogg"),
    ("AAC files", ".aac"),
    ("M4A files", ".m4a"),
    ("WMA files", ".wma"),
    ("Opus files", ".opus"),
    ("AIFF files", ".aiff"),
    ("ALAC files", ".alac"),
    ("WavPack files", ".wv"),
    ("Monkey's Audio files", ".ape"),
    ("True Audio files", ".tta"),
    ("AC3 files", ".ac3"),
    ("DTS files", ".dts"),
    ("MP2 files", ".mp2"),
    ("Musepack files", ".mpc"),
    ("Speex files", ".spx"),
    ("AMR files", ".amr"),
    ("AU files", ".au"),
    ("Matroska Audio files", ".mka"),
    ("Ogg FLAC files", ".oga"),
    ("CAF files", ".caf"),
    ("Shorten files", ".shn"),
]

FORMAT_CODECS = {
    'mp3': 'libmp3lame',
    'flac': 'flac',
    'wav': 'pcm_s16le',
    'ogg': 'libvorbis',
    'aac': 'aac',
    'm4a': 'aac',
    'wma': 'wmav2',
    'opus': 'libopus',
    'aiff': 'pcm_s16be',
    'alac': 'alac',
    'wv': 'wavpack',
    'ape': 'ape',
    'tta': 'tta',
    'ac3': 'ac3',
    'dts': 'dts',
    'mp2': 'mp2',
    'mpc': 'mpc',
    'spx': 'libspeex',
    'amr': 'libopencore_amrnb',
    'au': 'pcm_s16be',
    'mka': 'libvorbis',
    'oga': 'flac',
    'caf': 'pcm_s16le',
    'shn': 'shorten',
}

QUALITY_PRESETS = {
    'mp3': ['320 kbps (CBR)', '256 kbps (CBR)', '192 kbps (CBR)', '128 kbps (CBR)', 'VBR Высшее (Q0)', 'VBR Высокое (Q2)', 'VBR Среднее (Q4)', 'VBR Низкое (Q6)'],
    'aac': ['320 kbps', '256 kbps', '192 kbps', '128 kbps'],
    'm4a': ['320 kbps', '256 kbps', '192 kbps', '128 kbps'],
    'ogg': ['Качество 10 (макс)', 'Качество 8 (высокое)', 'Качество 6 (среднее)', 'Качество 4 (низкое)', 'Качество 2 (мин)'],
    'opus': ['256 kbps', '192 kbps', '128 kbps', '96 kbps', '64 kbps'],
    'wma': ['320 kbps', '256 kbps', '192 kbps', '128 kbps'],
}

DEFAULT_TEMPLATES = [
    'VK_{n:03d}_custom',
    'modified_{original}',
    '{artist} - {title}',
    '{title}',
    '{original}',
    '{n:03d} - {artist} - {title}',
    '{artist} - {album} - {n:02d} - {title}',
    '{year} - {artist} - {title}',
    '[VK] {title}',
    '{title} (modified)',
]


class BatchConverter:
    def __init__(self, files, output_dir, output_format, quality_preset,
                 result_queue, max_workers=4, delete_originals=False):
        self.files = files
        self.output_dir = output_dir
        self.output_format = output_format
        self.quality_preset = quality_preset
        self.queue = result_queue
        self.max_workers = max_workers
        self.delete_originals = delete_originals
        self._success_count = 0
        self._lock = threading.Lock()

    def run_in_thread(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _get_ffmpeg_args(self, input_path, output_path):
        codec = FORMAT_CODECS.get(self.output_format, 'libmp3lame')
        args = ['ffmpeg', '-i', input_path]

        if self.output_format == 'mp3':
            if 'CBR' in self.quality_preset:
                bitrate = self.quality_preset.split()[0]
                args.extend(['-codec:a', codec, '-b:a', f'{bitrate}k'])
            else:
                if 'Q0' in self.quality_preset:
                    args.extend(['-codec:a', codec, '-q:a', '0'])
                elif 'Q2' in self.quality_preset:
                    args.extend(['-codec:a', codec, '-q:a', '2'])
                elif 'Q4' in self.quality_preset:
                    args.extend(['-codec:a', codec, '-q:a', '4'])
                elif 'Q6' in self.quality_preset:
                    args.extend(['-codec:a', codec, '-q:a', '6'])
                else:
                    args.extend(['-codec:a', codec, '-q:a', '0'])
        elif self.output_format in ['aac', 'm4a', 'opus', 'wma']:
            bitrate = self.quality_preset.split()[0]
            args.extend(['-codec:a', codec, '-b:a', f'{bitrate}k'])
        elif self.output_format == 'ogg':
            if '10' in self.quality_preset:
                args.extend(['-codec:a', codec, '-q:a', '10'])
            elif '8' in self.quality_preset:
                args.extend(['-codec:a', codec, '-q:a', '8'])
            elif '6' in self.quality_preset:
                args.extend(['-codec:a', codec, '-q:a', '6'])
            elif '4' in self.quality_preset:
                args.extend(['-codec:a', codec, '-q:a', '4'])
            elif '2' in self.quality_preset:
                args.extend(['-codec:a', codec, '-q:a', '2'])
            else:
                args.extend(['-codec:a', codec, '-q:a', '6'])
        elif self.output_format == 'flac':
            if 'Compression' in self.quality_preset:
                comp = self.quality_preset.split()[-1]
                args.extend(['-codec:a', codec, '-compression_level', comp])
            else:
                args.extend(['-codec:a', codec])
        elif self.output_format in ['wav', 'aiff', 'alac', 'wv', 'ape', 'tta', 'au', 'oga', 'caf', 'shn']:
            args.extend(['-codec:a', codec])
        elif self.output_format == 'ac3':
            args.extend(['-codec:a', codec, '-b:a', '448k'])
        elif self.output_format == 'dts':
            args.extend(['-codec:a', codec, '-b:a', '1536k'])
        elif self.output_format == 'mp2':
            args.extend(['-codec:a', codec, '-b:a', '256k'])
        elif self.output_format == 'mpc':
            args.extend(['-codec:a', codec, '-q:a', '7'])
        elif self.output_format == 'spx':
            args.extend(['-codec:a', codec, '-q:a', '8'])
        elif self.output_format == 'amr':
            args.extend(['-codec:a', codec, '-ar', '8000', '-ac', '1', '-b:a', '12.2k'])
        elif self.output_format == 'mka':
            args.extend(['-codec:a', codec, '-q:a', '6'])
        else:
            args.extend(['-codec:a', 'libmp3lame', '-b:a', '320k'])

        args.extend(['-y', output_path])
        return args

    def _process_one(self, idx, file_path):
        total = len(self.files)
        self.queue.put(('progress', idx + 1, total, file_path))

        try:
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            output_name = f"{base_name}.{self.output_format}"
            output_path = os.path.join(self.output_dir, output_name)

            counter = 1
            while os.path.exists(output_path):
                output_name = f"{base_name}_{counter}.{self.output_format}"
                output_path = os.path.join(self.output_dir, output_name)
                counter += 1

            args = self._get_ffmpeg_args(file_path, output_path)
            result = subprocess.run(args, capture_output=True, encoding='utf-8', errors='ignore', timeout=300)

            if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                if self.delete_originals:
                    try:
                        os.unlink(file_path)
                    except Exception:
                        pass
                with self._lock:
                    self._success_count += 1
                self.queue.put(('file_done', file_path, True, output_path))
            else:
                self.queue.put(('file_done', file_path, False, ""))
        except Exception as e:
            self.queue.put(('file_done', file_path, False, ""))
            self.queue.put(('error', f"Ошибка конвертации {os.path.basename(file_path)}: {str(e)}"))

    def _run(self):
        total = len(self.files)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_one, i, fp): i
                for i, fp in enumerate(self.files)
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.queue.put(('error', str(e)))
        self.queue.put(('all_done', self._success_count, total))


class VKModifierApp:
    def __init__(self, root):
        self.root = root
        self.root.title("VK Modifier")
        self.root.geometry("1250x950")
        self.root.minsize(1000, 800)

        self.input_files = []
        self.tracks_info = []
        self.current_index = -1
        self.output_dir = os.path.expanduser("~/Desktop/Output")
        self.saved_presets = []
        self.selected_cover_path = None
        self._cover_is_temp = False
        self._worker_queue = queue.Queue()
        self._waveform_samples = None
        self._waveform_loading = False
        self._preview_timer = None
        self._mode = 'modifier'
        self.user_templates = []
        self._selected_template_index = None
        self._wave_zoom = 1.0
        self._wave_offset = 0.0
        self._wave_drag_start = None

        self._load_config()
        self._create_vars()
        self._build_ui()
        self._setup_hotkeys()
        self._setup_drop_targets()
        self._completed_count = 0
        self.ffmpeg_ok = self._check_ffmpeg()
        self._log(f"FFmpeg: {'найден' if self.ffmpeg_ok else 'НЕ НАЙДЕН'}", 'info' if self.ffmpeg_ok else 'error')

    def _create_vars(self):
        self.v_pitch = tk.BooleanVar()
        self.v_pitch_val = tk.DoubleVar(value=0.5)
        self.v_speed = tk.BooleanVar()
        self.v_speed_val = tk.DoubleVar(value=1.00)
        self.v_eq = tk.BooleanVar()
        self.v_eq_type = tk.IntVar(value=0)
        self.v_eq_val = tk.DoubleVar(value=-2.0)
        self.v_silence = tk.BooleanVar()
        self.v_silence_val = tk.IntVar(value=45)
        self.v_phase_inv = tk.BooleanVar(value=True)
        self.v_phase_inv_val = tk.DoubleVar(value=1.0)
        self.v_phase_scr = tk.BooleanVar(value=True)
        self.v_phase_scr_val = tk.DoubleVar(value=2.0)
        self.v_dc = tk.BooleanVar(value=True)
        self.v_dc_val = tk.DoubleVar(value=0.000005)
        self.v_resamp = tk.BooleanVar(value=True)
        self.v_resamp_val = tk.IntVar(value=1)
        self.v_ultra = tk.BooleanVar(value=True)
        self.v_ultra_freq = tk.IntVar(value=21000)
        self.v_ultra_level = tk.DoubleVar(value=0.001)
        self.v_haas = tk.BooleanVar(value=True)
        self.v_haas_val = tk.DoubleVar(value=15.0)
        self.v_dither = tk.BooleanVar(value=True)
        self.v_dither_method = tk.StringVar(value='triangular_hp')
        self.v_id3pad = tk.BooleanVar(value=True)
        self.v_id3pad_val = tk.IntVar(value=512)

        self.v_spectral_mask = tk.BooleanVar(value=False)
        self.v_spectral_mask_sens = tk.DoubleVar(value=0.8)
        self.v_spectral_mask_att = tk.IntVar(value=12)
        self.v_spectral_mask_peaks = tk.IntVar(value=10)
        self.v_concert_emu = tk.BooleanVar(value=False)
        self.v_concert_intensity = tk.StringVar(value='medium')
        self.v_midside = tk.BooleanVar(value=False)
        self.v_midside_mid = tk.DoubleVar(value=-3.0)
        self.v_midside_side = tk.DoubleVar(value=2.0)
        self.v_psycho_noise = tk.BooleanVar(value=False)
        self.v_psycho_intensity = tk.DoubleVar(value=0.0003)
        self.v_saturation = tk.BooleanVar(value=False)
        self.v_saturation_drive = tk.DoubleVar(value=1.5)
        self.v_saturation_mix = tk.DoubleVar(value=0.15)
        self.v_temp_jitter = tk.BooleanVar(value=False)
        self.v_jitter_intensity = tk.DoubleVar(value=0.002)
        self.v_jitter_freq = tk.DoubleVar(value=0.5)
        self.v_spec_jitter = tk.BooleanVar(value=False)
        self.v_spec_jitter_count = tk.IntVar(value=5)
        self.v_spec_jitter_att = tk.IntVar(value=15)
        self.v_vk_infra = tk.BooleanVar(value=False)
        self.v_vk_infra_mode = tk.StringVar(value='modulated')
        self.v_vk_infra_amplitude = tk.DoubleVar(value=0.35)
        self.v_vk_infra_freq = tk.DoubleVar(value=18.0)
        self.v_vk_infra_mod_freq = tk.DoubleVar(value=0.08)
        self.v_vk_infra_mod_depth = tk.DoubleVar(value=0.3)
        self.v_vk_infra_phase_shift = tk.DoubleVar(value=0.0)
        self.v_vk_infra_waveform = tk.StringVar(value='sine')
        self.v_vk_infra_adaptive = tk.BooleanVar(value=True)
        self.v_vk_infra_h1 = tk.DoubleVar(value=0.15)
        self.v_vk_infra_h2 = tk.DoubleVar(value=0.07)
        self.v_vk_infra_h3 = tk.DoubleVar(value=0.03)

        self.v_trim = tk.BooleanVar()
        self.v_trim_val = tk.DoubleVar(value=5.0)
        self.v_cut = tk.BooleanVar()
        self.v_cut_pos = tk.IntVar(value=50)
        self.v_cut_dur = tk.DoubleVar(value=2.0)
        self.v_fade = tk.BooleanVar()
        self.v_fade_val = tk.DoubleVar(value=5.0)
        self.v_merge = tk.BooleanVar()
        self.v_extra = tk.StringVar()
        self.v_broken = tk.BooleanVar()
        self.v_broken_t = tk.IntVar(value=0)
        self.v_bitrate_j = tk.BooleanVar()
        self.v_frame_sh = tk.BooleanVar()
        self.v_fake_meta = tk.BooleanVar()
        self.v_reorder = tk.BooleanVar(value=True)
        self.v_preserve_meta = tk.BooleanVar()
        self.v_preserve_cover = tk.BooleanVar()
        self.v_rename = tk.BooleanVar(value=True)
        self.v_delete_orig = tk.BooleanVar()
        self.v_quality = tk.StringVar(value='320 kbps (CBR)')
        self.v_title = tk.StringVar()
        self.v_artist = tk.StringVar()
        self.v_album = tk.StringVar()
        self.v_year = tk.StringVar()
        self.v_genre = tk.StringVar()
        self.v_filename_template = tk.StringVar(value='VK_{n:03d}_custom')
        self.v_preset_name = tk.StringVar()
        self.v_max_workers = tk.IntVar(value=4)
        self.v_thread_delay = tk.DoubleVar(value=0.0)

        self.v_conv_format = tk.StringVar(value='mp3')
        self.v_conv_quality = tk.StringVar(value='320 kbps (CBR)')
        self.v_conv_delete = tk.BooleanVar()

    def _build_ui(self):
        top = ttk.Frame(self.root)
        top.pack(fill='x', padx=4, pady=2)

        ttk.Label(top, text="VK Modifier", font=('', 13, 'bold')).pack(side='left', padx=4)

        mode_frame = ttk.Frame(top)
        mode_frame.pack(side='left', padx=20)
        ttk.Button(mode_frame, text="Модификатор", command=lambda: self._switch_mode('modifier')).pack(side='left', padx=2)
        ttk.Button(mode_frame, text="Конвертер", command=lambda: self._switch_mode('converter')).pack(side='left', padx=2)

        self.lbl_mode = ttk.Label(top, text="Режим: Модификатор", font=('', 9, 'bold'), foreground='#6366f1')
        self.lbl_mode.pack(side='left', padx=10)

        self.lbl_ffmpeg = ttk.Label(top, text="FFmpeg: проверка...")
        self.lbl_ffmpeg.pack(side='right', padx=8)

        ttk.Separator(self.root, orient='horizontal').pack(fill='x')

        pw = ttk.PanedWindow(self.root, orient='horizontal')
        pw.pack(fill='both', expand=True, padx=4, pady=4)

        left = ttk.Frame(pw, width=320)
        left.pack_propagate(False)
        pw.add(left, weight=0)
        self._build_left(left)

        right_outer = ttk.Frame(pw)
        pw.add(right_outer, weight=1)
        self._build_right(right_outer)

        pw.bind('<Configure>', lambda e: self._on_pane_resize(e))

    def _switch_mode(self, mode):
        self._mode = mode
        if mode == 'modifier':
            self.modifier_frame.pack(fill='both', expand=True)
            self.converter_frame.pack_forget()
            self.lbl_mode.config(text="Режим: Модификатор", foreground='#6366f1')
        else:
            self.modifier_frame.pack_forget()
            self.converter_frame.pack(fill='both', expand=True)
            self.lbl_mode.config(text="Режим: Конвертер", foreground='#f59e0b')

        self._clear_files()
        self._log(f"Переключение в режим: {'Модификатор' if mode == 'modifier' else 'Конвертер'}", 'info')

    def _build_left(self, parent):
        btn_row = ttk.Frame(parent)
        btn_row.pack(fill='x', padx=4, pady=4)
        ttk.Button(btn_row, text="Добавить файлы", command=self._add_files_dialog).pack(side='left', fill='x', expand=True)
        ttk.Button(btn_row, text="Очистить", command=self._clear_files).pack(side='left', padx=2)

        list_frame = ttk.Frame(parent)
        list_frame.pack(fill='both', expand=True, padx=4)
        sb = ttk.Scrollbar(list_frame, orient='vertical')
        self.file_listbox = tk.Listbox(
            list_frame, yscrollcommand=sb.set, activestyle='dotbox',
            selectbackground='#6366f1', selectforeground='white', exportselection=False
        )
        sb.config(command=self.file_listbox.yview)
        sb.pack(side='right', fill='y')
        self.file_listbox.pack(side='left', fill='both', expand=True)
        self.file_listbox.bind('<<ListboxSelect>>', self._on_file_select)

        self.btn_remove = ttk.Button(parent, text="Удалить выбранный", command=self._remove_selected, state='disabled')
        self.btn_remove.pack(fill='x', padx=4, pady=2)

        self.lbl_stats = ttk.Label(parent, text="0 файлов | 0.0 MB", relief='sunken', anchor='w')
        self.lbl_stats.pack(fill='x', padx=4, pady=2)

    def _build_right(self, parent):
        self.modifier_frame = ttk.Frame(parent)
        self.converter_frame = ttk.Frame(parent)

        self._build_modifier_interface()
        self._build_converter_interface()

        self.modifier_frame.pack(fill='both', expand=True)

    def _build_modifier_interface(self):
        main_container = ttk.Frame(self.modifier_frame)
        main_container.pack(fill='both', expand=True)

        canvas = tk.Canvas(main_container, borderwidth=0, highlightthickness=0)
        vsb = ttk.Scrollbar(main_container, orient='vertical', command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)

        canvas.pack(side='left', fill='both', expand=True)
        vsb.pack(side='right', fill='y')

        self._scroll_frame = ttk.Frame(canvas)
        fid = canvas.create_window((0, 0), window=self._scroll_frame, anchor='nw')

        def _on_frame_cfg(e):
            canvas.configure(scrollregion=canvas.bbox('all'))

        def _on_canvas_cfg(e):
            canvas.itemconfig(fid, width=e.width)

        def _on_wheel(e):
            canvas.yview_scroll(int(-1 * (e.delta / 120)), 'units')

        self._scroll_frame.bind('<Configure>', _on_frame_cfg)
        canvas.bind('<Configure>', _on_canvas_cfg)

        def _bind_wheel(event):
            canvas.bind_all('<MouseWheel>', _on_wheel)

        def _unbind_wheel(event):
            canvas.unbind_all('<MouseWheel>')

        canvas.bind('<Enter>', _bind_wheel)
        canvas.bind('<Leave>', _unbind_wheel)

        f = self._scroll_frame
        p = dict(padx=6, pady=4)

        row1 = ttk.Frame(f)
        row1.pack(fill='x', **p)
        self._build_cover_section(row1)
        self._build_metadata_section(row1)
        self._build_track_info_section(row1)

        self._build_waveform_section(f)
        self._build_methods_notebook()

        self.lbl_conflict = ttk.Label(f, text="", foreground='red', wraplength=800, justify='left')
        self.lbl_conflict.pack(fill='x', padx=6)

        row3 = ttk.Frame(f)
        row3.pack(fill='x', **p)
        self._build_output_section(row3)
        self._build_preset_management(row3)

        self._build_action_section(f)
        self._build_log_section(f)

    def _build_converter_interface(self):
        f = self.converter_frame

        header = ttk.Frame(f)
        header.pack(fill='x', padx=6, pady=4)
        ttk.Label(header, text="Конвертер аудиоформатов", font=('', 12, 'bold')).pack(side='left')
        ttk.Label(header, text="Поддерживается 26 форматов", foreground='#888', font=('', 9)).pack(side='right')

        ttk.Separator(f, orient='horizontal').pack(fill='x', padx=6)

        settings_frame = ttk.LabelFrame(f, text="Настройки конвертации", padding=8)
        settings_frame.pack(fill='x', padx=6, pady=4)

        fmt_row = ttk.Frame(settings_frame)
        fmt_row.pack(fill='x', pady=4)
        ttk.Label(fmt_row, text="Выходной формат:", font=('', 9, 'bold')).pack(side='left', padx=4)

        self.cmb_conv_format = ttk.Combobox(fmt_row, textvariable=self.v_conv_format,
                                            values=list(SUPPORTED_FORMATS.keys()), width=10, state='readonly')
        self.cmb_conv_format.pack(side='left', padx=4)
        self.cmb_conv_format.bind('<<ComboboxSelected>>', self._on_format_changed)

        self.lbl_format_desc = ttk.Label(fmt_row, text="", foreground='#888', font=('', 8))
        self.lbl_format_desc.pack(side='left', padx=10)

        self.quality_frame = ttk.Frame(settings_frame)
        self.quality_frame.pack(fill='x', pady=4)
        ttk.Label(self.quality_frame, text="Качество:", font=('', 9, 'bold')).pack(side='left', padx=4)

        self.cmb_conv_quality = ttk.Combobox(self.quality_frame, textvariable=self.v_conv_quality, width=25, state='readonly')
        self.cmb_conv_quality.pack(side='left', padx=4)

        out_frame = ttk.LabelFrame(f, text="Настройки вывода", padding=8)
        out_frame.pack(fill='x', padx=6, pady=4)

        dir_row = ttk.Frame(out_frame)
        dir_row.pack(fill='x', pady=2)
        ttk.Button(dir_row, text="Выбрать папку", command=self._select_output_dir).pack(side='left')
        self.lbl_out_dir_conv = ttk.Label(dir_row, text=self.output_dir, relief='sunken', padding=2, width=30)
        self.lbl_out_dir_conv.pack(side='left', padx=4, fill='x', expand=True)

        ttk.Checkbutton(out_frame, text="Удалять оригиналы после конвертации", variable=self.v_conv_delete).pack(anchor='w', pady=2)

        info_frame = ttk.LabelFrame(f, text="Поддерживаемые форматы", padding=4)
        info_frame.pack(fill='x', padx=6, pady=2)

        info_text = (
            "Lossy: MP3, AAC/M4A, OGG, Opus, WMA, AC3, DTS, MP2, MPC, Speex, AMR\n"
            "Lossless: FLAC, WAV, AIFF, ALAC, WV, APE, TTA, SHN, OGG FLAC\n"
            "Другие: MKA, CAF, AU"
        )
        self.format_info_label = ttk.Label(info_frame, text=info_text, font=('Courier', 8), justify='left')
        self.format_info_label.pack(fill='x')
        self._on_format_changed()

        action_frame = ttk.Frame(f)
        action_frame.pack(fill='x', padx=6, pady=4)

        self.conv_progress_var = tk.IntVar()
        self.conv_progress_bar = ttk.Progressbar(action_frame, variable=self.conv_progress_var, maximum=100)
        self.conv_progress_bar.pack(side='left', fill='x', expand=True, padx=(0, 8))

        self.btn_convert = ttk.Button(action_frame, text="Запустить конвертацию", command=self._start_conversion)
        self.btn_convert.pack(side='right')

        log_frame = ttk.LabelFrame(f, text="Лог конвертации", padding=4)
        log_frame.pack(fill='both', expand=True, padx=6, pady=(0, 6))

        self.conv_log_text = scrolledtext.ScrolledText(log_frame, height=8, state='disabled', font=('Courier', 9), wrap='word')
        self.conv_log_text.pack(fill='both', expand=True)
        self.conv_log_text.tag_config('info', foreground='#333333')
        self.conv_log_text.tag_config('success', foreground='#007700')
        self.conv_log_text.tag_config('warning', foreground='#aa6600')
        self.conv_log_text.tag_config('error', foreground='#cc0000')

    def _on_format_changed(self, event=None):
        fmt = self.v_conv_format.get()
        desc = SUPPORTED_FORMATS.get(fmt, '')
        self.lbl_format_desc.config(text=desc)

        if fmt in QUALITY_PRESETS:
            self.cmb_conv_quality['values'] = QUALITY_PRESETS[fmt]
            default_val = '320 kbps (CBR)' if '320 kbps (CBR)' in QUALITY_PRESETS[fmt] else QUALITY_PRESETS[fmt][0]
            self.v_conv_quality.set(default_val)
            self.cmb_conv_quality.config(state='readonly')
        elif fmt in ['wav', 'aiff', 'au', 'caf']:
            self.cmb_conv_quality['values'] = ['Uncompressed PCM']
            self.v_conv_quality.set('Uncompressed PCM')
            self.cmb_conv_quality.config(state='disabled')
        elif fmt == 'flac':
            self.cmb_conv_quality['values'] = [
                'Compression 0 (fast)', 'Compression 5 (default)',
                'Compression 8 (best)', 'Compression 12 (max)'
            ]
            self.v_conv_quality.set('Compression 5 (default)')
            self.cmb_conv_quality.config(state='readonly')
        elif fmt in ['alac', 'wv', 'ape', 'tta', 'shn']:
            self.cmb_conv_quality['values'] = ['Lossless / Default']
            self.v_conv_quality.set('Lossless / Default')
            self.cmb_conv_quality.config(state='disabled')
        elif fmt in ['ac3', 'dts', 'mp2', 'mpc', 'spx', 'amr', 'mka', 'oga']:
            self.cmb_conv_quality['values'] = ['Default quality']
            self.v_conv_quality.set('Default quality')
            self.cmb_conv_quality.config(state='disabled')

    def _start_conversion(self):
        if not self.input_files:
            messagebox.showwarning("Внимание", "Добавьте аудиофайлы для конвертации")
            return
        if not self.output_dir:
            messagebox.showwarning("Внимание", "Выберите папку для сохранения")
            return
        if not self.ffmpeg_ok:
            messagebox.showerror("Ошибка", "FFmpeg не найден!")
            return

        os.makedirs(self.output_dir, exist_ok=True)
        self.btn_convert.config(state='disabled')
        self.conv_progress_var.set(0)
        self.conv_progress_bar.config(maximum=len(self.input_files))
        self._completed_count = 0

        output_format = self.v_conv_format.get()
        quality = self.v_conv_quality.get()
        max_workers = self.v_max_workers.get()

        self._log(f"Запущена конвертация {len(self.input_files)} файлов в {output_format.upper()}...", 'info', to_converter=True)

        converter = BatchConverter(
            files=list(self.input_files), output_dir=self.output_dir,
            output_format=output_format, quality_preset=quality,
            result_queue=self._worker_queue, max_workers=max_workers,
            delete_originals=self.v_conv_delete.get()
        )
        converter.run_in_thread()
        self._poll_converter_queue()

    def _poll_converter_queue(self):
        try:
            while True:
                msg = self._worker_queue.get_nowait()
                kind = msg[0]

                if kind == 'progress':
                    _, cur, tot, fp = msg
                    self._log(f"[{cur}/{tot}] {os.path.basename(fp)}", 'info', to_converter=True)
                elif kind == 'file_done':
                    _, fp, ok, out = msg
                    self._completed_count += 1
                    self.conv_progress_var.set(self._completed_count)
                    if ok:
                        self._log(f"OK {os.path.basename(fp)} -> {os.path.basename(out)}", 'success', to_converter=True)
                    else:
                        self._log(f"ERROR {os.path.basename(fp)}", 'error', to_converter=True)
                elif kind == 'all_done':
                    _, sc, tot = msg
                    self.btn_convert.config(state='normal')
                    self._log(f"Конвертация завершена: {sc}/{tot} файлов", 'success', to_converter=True)
                    messagebox.showinfo("Готово", f"Конвертировано {sc} из {tot} файлов")
                    return
                elif kind == 'error':
                    self._log(f"ERROR: {msg[1]}", 'error', to_converter=True)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_converter_queue)

    def _build_cover_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Обложка", padding=6)
        lf.pack(side='left', fill='y', padx=(0, 4))

        self.lbl_cover = ttk.Label(lf, text="(нет)", width=20, anchor='center', relief='groove', padding=4)
        self.lbl_cover.pack()

        ttk.Button(lf, text="Загрузить", command=self._select_cover).pack(fill='x', pady=1)
        ttk.Button(lf, text="Рандом", command=self._random_cover).pack(fill='x', pady=1)
        self.btn_rm_cover = ttk.Button(lf, text="Удалить", command=self._remove_cover, state='disabled')
        self.btn_rm_cover.pack(fill='x', pady=1)

    def _build_metadata_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Метаданные", padding=6)
        lf.pack(side='left', fill='both', expand=True, padx=(0, 4))

        fields = [
            ("Название", self.v_title),
            ("Исполнитель", self.v_artist),
            ("Альбом", self.v_album),
            ("Год", self.v_year),
            ("Жанр", self.v_genre)
        ]
        for row_i, (lbl, var) in enumerate(fields):
            ttk.Label(lf, text=lbl).grid(row=row_i, column=0, sticky='w', padx=2, pady=1)
            e = ttk.Entry(lf, textvariable=var)
            e.grid(row=row_i, column=1, sticky='ew', padx=2, pady=1)
            var.trace_add('write', lambda *_: self._update_name_preview())

        lf.columnconfigure(1, weight=1)

        btn_row = ttk.Frame(lf)
        btn_row.grid(row=len(fields), column=0, columnspan=2, pady=4)
        ttk.Button(btn_row, text="Копировать из оригинала", command=self._copy_meta).pack(side='left', padx=2)
        ttk.Button(btn_row, text="Рандом", command=self._random_meta).pack(side='left', padx=2)
        ttk.Button(btn_row, text="Очистить", command=self._clear_meta).pack(side='left', padx=2)

        r = len(fields) + 1
        self.lbl_title_preview = ttk.Label(lf, text="Предпросмотр: —", foreground='#888', font=('', 8))
        self.lbl_title_preview.grid(row=r, column=0, columnspan=2, sticky='w', pady=(2, 0))
        self.lbl_filename_preview = ttk.Label(lf, text="Имя файла: —", foreground='#007700', font=('Consolas', 9))
        self.lbl_filename_preview.grid(row=r+1, column=0, columnspan=2, sticky='w', pady=(2, 0))

    def _update_name_preview(self):
        title_raw = self.v_title.get()
        artist = self.v_artist.get()
        album = self.v_album.get()
        year = self.v_year.get()

        display_title = title_raw if title_raw else "(нет названия)"
        tpl = self.v_filename_template.get() or 'VK_{n:03d}_custom'

        if self.current_index >= 0 and self.current_index < len(self.tracks_info):
            ti = self.tracks_info[self.current_index]
            orig = os.path.splitext(os.path.basename(self.input_files[self.current_index]))[0]
            ex_title = title_raw or ti.title or orig
            ex_artist = artist or ti.artist or ''
            ex_album = album or ti.album or ''
            ex_year = year or ti.year or ''
        else:
            orig = 'example_track'
            ex_title = title_raw or 'Example Song'
            ex_artist = artist or 'Example Artist'
            ex_album = album or 'Example Album'
            ex_year = year or '2024'

        try:
            fname_simple = tpl.format(
                n=1, original=self._safe_filename(orig), title=self._safe_filename(ex_title),
                artist=self._safe_filename(ex_artist), album=self._safe_filename(ex_album),
                year=self._safe_filename(str(ex_year))
            ) + '.mp3'
            self.lbl_filename_preview.config(text=f"Имя файла: {fname_simple}", foreground='#007700')
        except (KeyError, ValueError) as e:
            self.lbl_filename_preview.config(text=f"Ошибка шаблона: {e}", foreground='#cc0000')

        try:
            self.lbl_title_preview.config(text=f"Предпросмотр: {display_title}")
        except AttributeError:
            pass

    def _build_track_info_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Анализ трека", padding=6)
        lf.pack(side='left', fill='y')

        self.txt_track_info = tk.Text(lf, width=30, height=8, state='disabled', font=('Courier', 9), wrap='none')
        self.txt_track_info.pack(fill='both', expand=True)
        self._update_track_info(-1)

    def _build_methods_notebook(self, nb_parent=None):
        if nb_parent is None:
            nb = ttk.Notebook(self._scroll_frame)
            nb.pack(fill='x', padx=6, pady=4)
        else:
            nb = nb_parent

        self._build_basic_tab(nb)
        self._build_spectral_tab(nb)
        self._build_texture_tab(nb)
        self._build_advanced_tab(nb)
        self._build_technical_tab(nb)
        self._build_system_tab(nb)
        self._build_filename_templates_tab(nb)

        tracked_vars = [
            self.v_fade, self.v_fade_val, self.v_trim, self.v_trim_val,
            self.v_speed, self.v_speed_val, self.v_pitch, self.v_pitch_val,
            self.v_eq, self.v_eq_type, self.v_eq_val, self.v_silence, self.v_silence_val,
            self.v_phase_inv, self.v_phase_inv_val, self.v_phase_scr, self.v_phase_scr_val,
            self.v_dc, self.v_dc_val, self.v_resamp, self.v_resamp_val,
            self.v_ultra, self.v_ultra_level, self.v_haas, self.v_haas_val,
            self.v_cut, self.v_cut_pos, self.v_cut_dur, self.v_spectral_mask,
            self.v_spectral_mask_att, self.v_spectral_mask_peaks, self.v_concert_emu,
            self.v_concert_intensity, self.v_midside, self.v_midside_mid,
            self.v_midside_side, self.v_psycho_noise, self.v_psycho_intensity,
            self.v_temp_jitter, self.v_jitter_intensity, self.v_jitter_freq,
            self.v_spec_jitter, self.v_spec_jitter_count, self.v_spec_jitter_att,
            self.v_saturation, self.v_saturation_drive, self.v_saturation_mix
        ]
        for v in tracked_vars:
            v.trace_add('write', lambda *a: self._schedule_preview_update())

    def _build_filename_templates_tab(self, nb):
        f = ttk.Frame(nb, padding=8)
        nb.add(f, text="Имена")

        # Конструктор и сохранённые шаблоны
        mid_frame = ttk.Frame(f)
        mid_frame.pack(fill='both', expand=True, pady=6)

        left_column = ttk.Frame(mid_frame)
        left_column.pack(side='left', fill='both', expand=True, padx=(0, 4))

        list_frame = ttk.LabelFrame(left_column, text="💾 Сохранённые шаблоны", padding=4)
        list_frame.pack(side='top', fill='both', expand=True)

        sb_tpl = ttk.Scrollbar(list_frame, orient='vertical')
        self.template_listbox = tk.Listbox(list_frame, yscrollcommand=sb_tpl.set, height=12, exportselection=False,
                                           selectbackground='#6366f1', selectforeground='white')
        sb_tpl.config(command=self.template_listbox.yview)
        sb_tpl.pack(side='right', fill='y')
        self.template_listbox.pack(side='left', fill='both', expand=True)
        self.template_listbox.bind('<<ListboxSelect>>', self._on_template_select_auto)

        list_buttons_frame = ttk.Frame(left_column)
        list_buttons_frame.pack(side='top', fill='x', pady=(4, 0))
        ttk.Button(list_buttons_frame, text="Создать шаблон", command=self._create_template_dialog).pack(side='left', padx=2, expand=True, fill='x')
        ttk.Button(list_buttons_frame, text="Удалить", command=self._delete_selected_template).pack(side='left', padx=2, expand=True, fill='x')

        constr_frame = ttk.LabelFrame(mid_frame, text="Конструктор шаблона", padding=6)
        constr_frame.pack(side='right', fill='both', expand=True)

        ttk.Label(constr_frame, text="Шаблон:", font=('', 9, 'bold')).pack(anchor='w')
        self.text_template_pattern = tk.Text(constr_frame, font=('Consolas', 10), width=35, height=3, wrap='word', undo=True, autoseparators=True)
        self.text_template_pattern.pack(fill='x', pady=(2, 4))
        self.text_template_pattern.tag_configure('variable', background='#e0e7ff', foreground='#3730a3', borderwidth=1, relief='raised')
        self.text_template_pattern.tag_configure('variable_sel', background='#6366f1', foreground='white')

        self.text_template_pattern.bind('<KeyRelease>', self._on_text_template_change)
        self.text_template_pattern.bind('<KeyPress-BackSpace>', self._on_variable_backspace)
        self.text_template_pattern.bind('<KeyPress-Delete>', self._on_variable_delete)
        self.text_template_pattern.bind('<KeyPress-space>', self._on_text_template_change)
        self.text_template_pattern.bind('<KeyPress-Return>', self._on_text_template_change)

        vars_label = ttk.Label(constr_frame, text="Быстрая вставка переменных:", font=('', 8, 'bold'), foreground='#666')
        vars_label.pack(anchor='w', pady=(0, 2))

        vars_frame = ttk.Frame(constr_frame)
        vars_frame.pack(fill='x', pady=(0, 4))
        variables = [('{n}', 'Номер'), ('{n:03d}', 'Номер 001'), ('{original}', 'Ориг. имя'), ('{title}', 'Название'), ('{artist}', 'Артист'), ('{album}', 'Альбом'), ('{year}', 'Год')]
        for i, (var_text, var_desc) in enumerate(variables):
            btn = ttk.Button(vars_frame, text=var_text, width=10, command=lambda vt=var_text: self._insert_template_var_tagged(vt))
            btn.grid(row=i // 4, column=i % 4, padx=2, pady=2, sticky='ew')

        preview_live_frame = ttk.Frame(constr_frame, relief='sunken', borderwidth=1)
        preview_live_frame.pack(fill='x', pady=(8, 4))
        self.lbl_template_live_preview = ttk.Label(preview_live_frame, text="Предпросмотр: --", foreground='#333', font=('Consolas', 9), padding=4, anchor='w', justify='left')
        self.lbl_template_live_preview.pack(fill='x')

        self._refresh_template_list()
        self.v_filename_template.trace_add('write', lambda *_: self._update_name_preview())

    def _insert_template_var_tagged(self, var_text):
        try:
            self.text_template_pattern.insert('insert', var_text, 'variable')
            self._update_live_preview_from_text()
        except Exception:
            pass

    def _get_text_template_content(self):
        try:
            return self.text_template_pattern.get('1.0', 'end-1c')
        except Exception:
            return ''

    def _on_text_template_change(self, event=None):
        try:
            self._apply_variable_tags()
            self._update_live_preview_from_text()
            self._auto_update_selected_template()
        except Exception:
            pass

    def _auto_update_selected_template(self):
        if self._selected_template_index is None:
            return
        if self._selected_template_index >= len(self.user_templates):
            return
        pattern = self._get_text_template_content().strip()
        if not pattern:
            return
        try:
            pattern.format(n=1, original='test', title='test', artist='test', album='test', year='2024')
        except (KeyError, ValueError):
            return
        idx = self._selected_template_index
        self.user_templates[idx]['pattern'] = pattern
        self._refresh_template_list()
        self.template_listbox.selection_clear(0, tk.END)
        self.template_listbox.selection_set(idx)
        self.template_listbox.activate(idx)
        self._save_config()

    def _apply_variable_tags(self):
        try:
            content = self.text_template_pattern.get('1.0', 'end-1c')
            self.text_template_pattern.tag_remove('variable', '1.0', 'end')
            import re
            for match in re.finditer(r'\{[^}]+\}', content):
                self.text_template_pattern.tag_add('variable', f"1.0+{match.start()}c", f"1.0+{match.end()}c")
        except Exception:
            pass

    def _on_variable_backspace(self, event=None):
        try:
            cursor_pos = self.text_template_pattern.index('insert')
            line, col = map(int, cursor_pos.split('.'))
            line_content = self.text_template_pattern.get(f"{line}.0", f"{line}.end")
            import re
            for match in re.finditer(r'\{[^}]+\}', line_content):
                if match.end() == col:
                    self.text_template_pattern.delete(f"{line}.{match.start()}", f"{line}.{match.end()}")
                    self.text_template_pattern.mark_set('insert', f"{line}.{match.start()}")
                    self._update_live_preview_from_text()
                    return 'break'
            self._on_text_template_change()
            return None
        except Exception:
            return None

    def _on_variable_delete(self, event=None):
        try:
            cursor_pos = self.text_template_pattern.index('insert')
            line, col = map(int, cursor_pos.split('.'))
            line_content = self.text_template_pattern.get(f"{line}.0", f"{line}.end")
            import re
            for match in re.finditer(r'\{[^}]+\}', line_content):
                if match.start() == col:
                    self.text_template_pattern.delete(f"{line}.{match.start()}", f"{line}.{match.end()}")
                    self._update_live_preview_from_text()
                    return 'break'
            self._on_text_template_change()
            return None
        except Exception:
            return None

    def _update_live_preview_from_text(self):
        try:
            tpl = self._get_text_template_content()
            if not tpl.strip():
                self.lbl_template_live_preview.config(text="Предпросмотр: --")
                return

            if self.current_index >= 0 and self.current_index < len(self.tracks_info):
                ti = self.tracks_info[self.current_index]
                orig = os.path.splitext(os.path.basename(self.input_files[self.current_index]))[0]
                ex_title = self.v_title.get() or ti.title or orig
                ex_artist = self.v_artist.get() or ti.artist or ''
                ex_album = self.v_album.get() or ti.album or ''
                ex_year = self.v_year.get() or ti.year or ''
                n_val = self.current_index + 1
            else:
                orig = 'example_track'
                ex_title = 'Example Song'
                ex_artist = 'Example Artist'
                ex_album = 'Example Album'
                ex_year = '2024'
                n_val = 1

            fname = tpl.format(
                n=n_val, original=self._safe_filename(orig), title=self._safe_filename(ex_title),
                artist=self._safe_filename(ex_artist), album=self._safe_filename(ex_album),
                year=self._safe_filename(str(ex_year))
            ) + '.mp3'
            self.lbl_template_live_preview.config(text=f"Предпросмотр: {fname}", foreground='#007700')
        except (KeyError, ValueError) as e:
            self.lbl_template_live_preview.config(text=f"Ошибка: {e}", foreground='#cc0000')

    def _save_user_template(self):
        name = self.v_new_template_name.get().strip()
        pattern = self._get_text_template_content().strip()
        if not name:
            messagebox.showwarning("Внимание", "Введите имя шаблона")
            return
        if not pattern:
            messagebox.showwarning("Внимание", "Введите шаблон")
            return
        try:
            pattern.format(n=1, original='test', title='test', artist='test', album='test', year='2024')
        except (KeyError, ValueError) as e:
            messagebox.showerror("Ошибка", f"Некорректный шаблон:\n{e}")
            return

        for tpl in self.user_templates:
            if tpl['name'] == name:
                tpl['pattern'] = pattern
                self._refresh_template_list()
                self._save_config()
                self._log(f"Шаблон '{name}' обновлён", 'success')
                return

        self.user_templates.append({'name': name, 'pattern': pattern})
        self._refresh_template_list()
        self._save_config()
        self._log(f"Шаблон '{name}' сохранён", 'success')
        dialog.destroy()

    def _on_template_select_auto(self, event):
        sel = self.template_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx < len(self.user_templates):
            tpl = self.user_templates[idx]
            pattern = tpl['pattern']
            self.v_filename_template.set(pattern)
            self._selected_template_index = idx
            # Обновляем редактор шаблона
            self.text_template_pattern.delete('1.0', 'end')
            self.text_template_pattern.insert('1.0', pattern)
            self._apply_variable_tags()
            self._update_live_preview_from_text()
            self._log(f"Шаблон '{tpl['name']}' активирован", 'success')

    def _on_template_select(self, event):
        sel = self.template_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx < len(self.user_templates):
            tpl = self.user_templates[idx]
            self.text_template_pattern.delete('1.0', 'end')
            self.text_template_pattern.insert('1.0', tpl['pattern'])
            self._apply_variable_tags()
            self._update_live_preview_from_text()

    def _create_template_dialog(self):
        dialog = tk.Toplevel(self.root)
        dialog.title("Создать шаблон")
        dialog.geometry("350x180")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Название шаблона:").pack(pady=(10, 5))
        name_var = tk.StringVar()
        name_entry = ttk.Entry(dialog, textvariable=name_var, width=40)
        name_entry.pack(pady=5)

        ttk.Label(dialog, text="Шаблон (например, VK_{n:03d}_custom):").pack(pady=(5, 5))
        pattern_var = tk.StringVar()
        pattern_entry = ttk.Entry(dialog, textvariable=pattern_var, width=40)
        pattern_entry.pack(pady=5)

        def on_save():
            name = name_var.get().strip()
            pattern = pattern_var.get().strip()
            if not name:
                messagebox.showwarning("Внимание", "Введите название шаблона")
                return
            if not pattern:
                messagebox.showwarning("Внимание", "Введите шаблон")
                return
            try:
                pattern.format(n=1, original='test', title='test', artist='test', album='test', year='2024')
            except (KeyError, ValueError) as e:
                messagebox.showerror("Ошибка", f"Некорректный шаблон:\n{e}")
                return
            
            for tpl in self.user_templates:
                if tpl['name'] == name:
                    tpl['pattern'] = pattern
                    self._refresh_template_list()
                    self._save_config()
                    self._log(f"Шаблон '{name}' обновлён", 'success')
                    dialog.destroy()
                    return

            self.user_templates.append({'name': name, 'pattern': pattern})
            self._refresh_template_list()
            self._save_config()
            self._log(f"Шаблон '{name}' сохранён", 'success')
            dialog.destroy()

        ttk.Button(dialog, text="Сохранить", command=on_save).pack(pady=10)
        name_entry.focus()

    def _delete_selected_template(self):
        sel = self.template_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx < len(self.user_templates):
            name = self.user_templates[idx]['name']
            self.user_templates.pop(idx)
            if self._selected_template_index == idx:
                self._selected_template_index = None
            elif self._selected_template_index is not None and self._selected_template_index > idx:
                self._selected_template_index -= 1
            self._refresh_template_list()
            self._save_config()
            self._log(f"Шаблон '{name}' удалён", 'warning')

    def _create_preset_dialog(self):
        dialog = tk.Toplevel(self.root)
        dialog.title("Создать пресет")
        dialog.geometry("350x180")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Название пресета:").pack(pady=(10, 5))
        name_var = tk.StringVar()
        name_entry = ttk.Entry(dialog, textvariable=name_var, width=40)
        name_entry.pack(pady=5)

        ttk.Label(dialog, text="Описание (необязательно):").pack(pady=(5, 5))
        desc_var = tk.StringVar()
        desc_entry = ttk.Entry(dialog, textvariable=desc_var, width=40)
        desc_entry.pack(pady=5)

        def on_save():
            name = name_var.get().strip()
            desc = desc_var.get().strip()
            if not name:
                messagebox.showwarning("Внимание", "Введите название пресета")
                return
            data = {'name': name, 'date': datetime.now().isoformat(), 'settings': self._collect_settings()}
            if desc:
                data['description'] = desc
            self.saved_presets.append(data)
            self._refresh_preset_list()
            self._save_config()
            self._log(f"Пресет '{name}' сохранён", 'success')
            dialog.destroy()

        ttk.Button(dialog, text="Сохранить", command=on_save).pack(pady=10)
        name_entry.focus()

    def _refresh_template_list(self):
        self.template_listbox.delete(0, 'end')
        for tpl in self.user_templates:
            self.template_listbox.insert('end', f"{tpl['name']}  ->  {tpl['pattern']}")

    def _build_waveform_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Предпросмотр формы сигнала (Детальный просмотр)", padding=4)
        lf.pack(fill='both', expand=True, padx=6, pady=4)

        hdr = ttk.Frame(lf)
        hdr.pack(fill='x', pady=(0, 2))
        ttk.Label(hdr, text="ДО изменений", font=('', 8, 'bold'), foreground='#5599ff').pack(side='left', padx=6)
        self.lbl_wave_status = ttk.Label(hdr, text="Выберите файл", foreground='gray', font=('', 8))
        self.lbl_wave_status.pack(side='left', padx=20)
        ttk.Label(hdr, text="ПОСЛЕ изменений", font=('', 8, 'bold'), foreground='#44dd44').pack(side='right', padx=6)

        wave_row = ttk.Frame(lf)
        wave_row.pack(fill='both', expand=True)
        wave_row.columnconfigure(0, weight=1)
        wave_row.columnconfigure(1, weight=1)
        wave_row.rowconfigure(0, weight=1)

        self.canvas_before = tk.Canvas(wave_row, height=300, bg='#0d1117', highlightthickness=1, highlightbackground='#30304a')
        self.canvas_before.grid(row=0, column=0, sticky='nsew', padx=(2, 1), pady=2)

        self.canvas_after = tk.Canvas(wave_row, height=300, bg='#0d170d', highlightthickness=1, highlightbackground='#2a3a2a')
        self.canvas_after.grid(row=0, column=1, sticky='nsew', padx=(1, 2), pady=2)

        self.canvas_before.bind('<Configure>', lambda e: self._schedule_redraw())
        self.canvas_after.bind('<Configure>', lambda e: self._schedule_redraw())

        self.canvas_before.bind('<MouseWheel>', self._stop_event_propagation, add="+")
        self.canvas_before.bind('<MouseWheel>', self._on_wave_mousewheel)
        self.canvas_before.bind('<Button-4>', self._stop_event_propagation, add="+")
        self.canvas_before.bind('<Button-4>', self._on_wave_mousewheel)
        self.canvas_before.bind('<Button-5>', self._stop_event_propagation, add="+")
        self.canvas_before.bind('<Button-5>', self._on_wave_mousewheel)
        self.canvas_before.bind('<ButtonPress-1>', self._on_wave_press)
        self.canvas_before.bind('<B1-Motion>', self._on_wave_drag)
        self.canvas_before.bind('<ButtonRelease-1>', self._on_wave_release)

        self.root.bind('<KeyPress-Shift_L>', self._on_shift_press)
        self.root.bind('<KeyRelease-Shift_L>', self._on_shift_release)
        self.root.bind('<KeyPress-Shift_R>', self._on_shift_press)
        self.root.bind('<KeyRelease-Shift_R>', self._on_shift_release)
        self.root.bind('<KeyPress-plus>', self._on_zoom_key)
        self.root.bind('<KeyPress-minus>', self._on_zoom_key)
        self.root.bind('<KeyPress-equal>', self._on_zoom_key)
        self.root.bind('<KeyPress-underscore>', self._on_zoom_key)

        self._shift_pressed = False

        lbl_instructions = ttk.Label(lf, text="🖱️ Колесо: Прокрутка | Ctrl+Колесо/+-: Зум | Shift+Колесо: Быстрая прокрутка | ЛКМ: Перетаскивание", font=("Segoe UI", 9), foreground="#888")
        lbl_instructions.pack(anchor='w', pady=(5, 0))

    def _load_waveform_for_file(self, file_path):
        if self._waveform_loading:
            return
        self._waveform_loading = True
        self._waveform_samples = None
        self._draw_placeholder(self.canvas_before, 'Загрузка...')
        self._draw_placeholder(self.canvas_after, '')

        def _load():
            try:
                cmd = ['ffmpeg', '-i', file_path, '-f', 's16le', '-ac', '1', '-ar', '500', '-']
                res = subprocess.run(cmd, capture_output=True, timeout=60)
                if res.returncode == 0 and res.stdout:
                    n = len(res.stdout) // 2
                    raw = struct.unpack(f'{n}h', res.stdout)
                    samples = [s / 32768.0 for s in raw]
                    self._waveform_samples = samples
                    self.root.after(0, self._on_waveform_loaded)
                else:
                    self.root.after(0, lambda: self._draw_placeholder(self.canvas_before, 'Ошибка декодирования'))
            except Exception as e:
                self.root.after(0, lambda: self._draw_placeholder(self.canvas_before, f'Ошибка: {e}'))
            finally:
                self._waveform_loading = False

        threading.Thread(target=_load, daemon=True).start()

    def _on_waveform_loaded(self):
        self.lbl_wave_status.config(text='')
        self._draw_waveform(self.canvas_before, self._waveform_samples, '#5599ff')
        self._start_preview_computation()

    def _schedule_redraw(self):
        if self._waveform_samples is None:
            return
        if self._preview_timer:
            self.root.after_cancel(self._preview_timer)
        self._preview_timer = self.root.after(50, lambda: (
            self._draw_waveform(self.canvas_before, self._waveform_samples, '#5599ff'),
            self._start_preview_computation()
        ))

    def _schedule_preview_update(self):
        if self._waveform_samples is None:
            return
        if self._preview_timer:
            self.root.after_cancel(self._preview_timer)
        self._preview_timer = self.root.after(350, self._start_preview_computation)

    def _start_preview_computation(self):
        if self._waveform_samples is None:
            return
        try:
            snap = {
                'vk_infra': self.v_vk_infra.get(),
                'vk_infra_amp': self.v_vk_infra_amplitude.get(),
                'vk_infra_freq': self.v_vk_infra_freq.get(),
                'vk_infra_mode': self.v_vk_infra_mode.get(),
                'vk_infra_mod_freq': self.v_vk_infra_mod_freq.get(),
                'vk_infra_mod_depth': self.v_vk_infra_mod_depth.get(),
                'vk_infra_phase': self.v_vk_infra_phase_shift.get(),
                'vk_infra_waveform': self.v_vk_infra_waveform.get(),
                'vk_infra_harmonics': [self.v_vk_infra_h1.get(), self.v_vk_infra_h2.get(), self.v_vk_infra_h3.get()],
                'fade': self.v_fade.get(), 'fade_val': self.v_fade_val.get(),
                'trim': self.v_trim.get(), 'trim_val': self.v_trim_val.get(),
                'speed': self.v_speed.get(), 'speed_val': self.v_speed_val.get(),
                'pitch': self.v_pitch.get(), 'pitch_val': self.v_pitch_val.get(),
                'eq': self.v_eq.get(), 'eq_type': self.v_eq_type.get(), 'eq_val': self.v_eq_val.get(),
                'silence': self.v_silence.get(), 'silence_val': self.v_silence_val.get(),
                'phase_inv': self.v_phase_inv.get(), 'phase_inv_val': self.v_phase_inv_val.get(),
                'phase_scr': self.v_phase_scr.get(), 'phase_scr_val': self.v_phase_scr_val.get(),
                'dc': self.v_dc.get(), 'dc_val': self.v_dc_val.get(),
                'resamp': self.v_resamp.get(), 'resamp_val': self.v_resamp_val.get(),
                'ultra': self.v_ultra.get(), 'ultra_level': self.v_ultra_level.get(),
                'haas': self.v_haas.get(), 'haas_val': self.v_haas_val.get(),
                'saturation': self.v_saturation.get(), 'sat_drive': self.v_saturation_drive.get(), 'sat_mix': self.v_saturation_mix.get(),
                'cut': self.v_cut.get(), 'cut_pos': self.v_cut_pos.get(), 'cut_dur': self.v_cut_dur.get(),
                'spectral_mask': self.v_spectral_mask.get(), 'spectral_mask_att': self.v_spectral_mask_att.get(), 'spectral_mask_peaks': self.v_spectral_mask_peaks.get(),
                'concert': self.v_concert_emu.get(), 'concert_intensity': self.v_concert_intensity.get(),
                'midside': self.v_midside.get(), 'midside_mid': self.v_midside_mid.get(), 'midside_side': self.v_midside_side.get(),
                'psycho': self.v_psycho_noise.get(), 'psycho_intensity': self.v_psycho_intensity.get(),
                'temp_jitter': self.v_temp_jitter.get(), 'jitter_intensity': self.v_jitter_intensity.get(), 'jitter_freq': self.v_jitter_freq.get(),
                'spec_jitter': self.v_spec_jitter.get(), 'spec_jitter_count': self.v_spec_jitter_count.get(), 'spec_jitter_att': self.v_spec_jitter_att.get(),
            }
        except tk.TclError:
            return

        samples = self._waveform_samples

        def _compute():
            preview = _compute_preview_static(samples, snap)
            self.root.after(0, lambda: self._draw_waveform(self.canvas_after, preview, '#44dd44'))

        threading.Thread(target=_compute, daemon=True).start()

    def _draw_placeholder(self, canvas, text):
        canvas.delete('all')
        w = canvas.winfo_width() or 200
        h = canvas.winfo_height() or 80
        canvas.create_line(0, h // 2, w, h // 2, fill='#333', width=1)
        if text:
            canvas.create_text(w // 2, h // 2, text=text, fill='gray', font=('', 8))

    def _draw_waveform(self, canvas, samples, color):
        canvas.delete('all')
        w = canvas.winfo_width()
        h = canvas.winfo_height()
        if w <= 1 or not samples:
            return

        mid = h // 2
        margin = 2
        draw_h = mid - margin
        n = len(samples)

        zoom = max(1.0, self._wave_zoom)
        visible_samples = int(n / zoom)
        start_idx = int(self._wave_offset * (n - visible_samples)) if n > visible_samples else 0
        end_idx = min(start_idx + visible_samples, n)

        if end_idx <= start_idx:
            start_idx = 0
            end_idx = n

        view_samples = samples[start_idx:end_idx]
        if not view_samples:
            return

        canvas.create_line(0, mid, w, mid, fill='#2a2a2a', width=1)

        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)
        inner_color = f'#{int(r*0.55):02x}{int(g*0.55):02x}{int(b*0.55):02x}'

        for x in range(w):
            i0 = int(x * len(view_samples) / w)
            i1 = int((x + 1) * len(view_samples) / w)
            if i1 <= i0:
                i1 = i0 + 1
            chunk = view_samples[i0:min(i1, len(view_samples))]
            if not chunk:
                continue

            peak_pos = max(chunk)
            peak_neg = min(chunk)
            rms = (sum(s * s for s in chunk) / len(chunk)) ** 0.5

            peak_pos = max(0.0, min(1.0, peak_pos))
            peak_neg = min(0.0, max(-1.0, peak_neg))
            rms = min(1.0, rms)

            y_top = mid - int(peak_pos * draw_h)
            y_bot = mid - int(peak_neg * draw_h)
            y_rms_top = mid - int(rms * draw_h)
            y_rms_bot = mid + int(rms * draw_h)

            if y_top >= y_bot:
                y_bot = y_top + 1

            canvas.create_line(x, y_top, x, y_bot, fill=inner_color)
            if y_rms_top < y_rms_bot:
                canvas.create_line(x, y_rms_top, x, y_rms_bot, fill=color)

        if zoom > 1.0:
            canvas.create_text(10, 10, text=f"Zoom: {zoom:.1f}x", anchor='nw', fill='#666', font=('', 8))

    def _clear_waveforms(self):
        self._waveform_samples = None
        self._wave_zoom = 1.0
        self._wave_offset = 0.0
        self._draw_placeholder(self.canvas_before, 'Выберите файл')
        self._draw_placeholder(self.canvas_after, '')
        self.lbl_wave_status.config(text='Выберите файл')

    def _stop_event_propagation(self, event):
        return 'break'

    def _on_wave_mousewheel(self, event):
        if self._waveform_samples is None:
            return

        if hasattr(event, 'delta'):
            delta = event.delta
        elif event.num == 4:
            delta = 120
        elif event.num == 5:
            delta = -120
        else:
            return

        ctrl_pressed = (event.state & 0x0004) != 0

        if ctrl_pressed:
            zoom_step = 0.1
            if delta > 0:
                self._wave_zoom = min(10.0, self._wave_zoom + zoom_step)
            else:
                self._wave_zoom = max(1.0, self._wave_zoom - zoom_step)
            self._schedule_redraw()
        elif self._shift_pressed:
            nav_step = 0.05
            if delta > 0:
                self._wave_offset = max(0.0, self._wave_offset - nav_step)
            else:
                self._wave_offset = min(1.0, self._wave_offset + nav_step)
            self._schedule_redraw()

        return 'break'

    def _on_shift_press(self, event):
        self._shift_pressed = True

    def _on_shift_release(self, event):
        self._shift_pressed = False

    def _on_zoom_key(self, event):
        if self._waveform_samples is None:
            return
        zoom_step = 0.2
        if event.keysym in ('plus', 'equal'):
            self._wave_zoom = min(10.0, self._wave_zoom + zoom_step)
        elif event.keysym in ('minus', 'underscore'):
            self._wave_zoom = max(1.0, self._wave_zoom - zoom_step)
        self._schedule_redraw()

    def _on_wave_press(self, event):
        if self._waveform_samples is None:
            return
        self._wave_drag_start = event.x

    def _on_wave_drag(self, event):
        if self._waveform_samples is None or self._wave_drag_start is None:
            return
        dx = event.x - self._wave_drag_start
        w = self.canvas_before.winfo_width()
        if w <= 0:
            return

        n = len(self._waveform_samples)
        zoom = max(1.0, self._wave_zoom)
        visible_samples = int(n / zoom)
        if visible_samples >= n:
            return

        visible_fraction = visible_samples / n
        pixels_per_full_track = w / visible_fraction if visible_fraction > 0 else w
        offset_change = -dx / pixels_per_full_track
        self._wave_offset = max(0.0, min(1.0, self._wave_offset + offset_change))
        self._wave_drag_start = event.x
        self._schedule_redraw()

    def _on_wave_release(self, event):
        self._wave_drag_start = None

    def _build_basic_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Базовые")
        f.columnconfigure(0, weight=0)

        r = 0
        ttk.Checkbutton(f, text="Изменить тональность (Pitch Shift)", variable=self.v_pitch, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_pitch_val, -5.0, 5.0, 0.5).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="семитонов").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Транспонирует аудио на +/-N полутонов без изменения темпа.")
        r += 1

        ttk.Checkbutton(f, text="Изменить скорость (Time Stretch)", variable=self.v_speed, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_speed_val, 0.90, 1.10, 0.01).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="x").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Ускоряет или замедляет трек с сохранением тональности.")
        r += 1

        ttk.Checkbutton(f, text="Эквализация (EQ)", variable=self.v_eq, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        eq_types = ["Стандарт: -2dB на 1 kHz", "Пресет Mid-Cut", "Пресет Air: 8k +3dB"]
        self.cmb_eq_type = ttk.Combobox(f, values=eq_types, width=26, state='readonly')
        self.cmb_eq_type.current(0)
        self.cmb_eq_type.grid(row=r, column=1, columnspan=2, padx=4, pady=(4, 0))
        self._spin(f, self.v_eq_val, -12.0, 12.0, 1.0, width=5).grid(row=r, column=3, padx=2, pady=(4, 0))
        ttk.Label(f, text="dB").grid(row=r, column=4, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Ослабляет или усиливает выбранную частотную полосу.")
        r += 1

        ttk.Checkbutton(f, text="Добавить тишину в конец (Silent Pad)", variable=self.v_silence, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_silence_val, 1, 300, 1, width=6, fmt=None).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="сек").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Добавляет тишину в конец файла.")

        for v in (self.v_pitch_val, self.v_speed_val, self.v_eq_val, self.v_silence_val):
            v.trace_add('write', lambda *a: self._check_conflicts())

    def _build_spectral_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Спектральные")

        rows_data = [
            ("Phase Invert", self.v_phase_inv, self._spin(f, self.v_phase_inv_val, 0.0, 1.0, 0.1), "сила", "Инвертирует фазу правого канала."),
            ("Phase Scramble", self.v_phase_scr, self._spin(f, self.v_phase_scr_val, 0.1, 5.0, 0.1), "Гц", "Синусоидальная модуляция фазы."),
            ("DC Shift", self.v_dc, self._spin(f, self.v_dc_val, 0.0, 0.0001, 0.000001, fmt='%.6f'), "", "Постоянное смещение сэмплов."),
            ("Resample Drift", self.v_resamp, self._spin(f, self.v_resamp_val, -100, 100, 1, fmt=None), "Гц", "Дрейф частоты дискретизации."),
            ("Haas Delay", self.v_haas, self._spin(f, self.v_haas_val, 0.0, 50.0, 0.5), "мс", "Задержка правого канала."),
        ]

        r = 0
        for title, var, spin, unit, desc in rows_data:
            ttk.Checkbutton(f, text=title, variable=var, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
            spin.grid(row=r, column=1, padx=4, pady=(4, 0))
            if unit:
                ttk.Label(f, text=unit).grid(row=r, column=2, sticky='w', pady=(4, 0))
            r += 1
            self._desc(f, r, 0, desc)
            r += 1

        ttk.Checkbutton(f, text="Ultrasonic Noise", variable=self.v_ultra, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        sub_u = ttk.Frame(f)
        sub_u.grid(row=r, column=1, columnspan=3, sticky='w', pady=(4, 0))
        ttk.Label(sub_u, text="Freq:").pack(side='left')
        self._spin(sub_u, self.v_ultra_freq, 20000, 48000, 100, width=7, fmt=None).pack(side='left', padx=2)
        ttk.Label(sub_u, text="Hz  Level:").pack(side='left')
        self._spin(sub_u, self.v_ultra_level, 0.0, 0.01, 0.0001, fmt='%.4f').pack(side='left', padx=2)
        r += 1
        self._desc(f, r, 0, "Подмешивает неслышимый ультразвук.")
        r += 1

        ttk.Checkbutton(f, text="Dither Attack", variable=self.v_dither, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        ttk.Combobox(f, textvariable=self.v_dither_method, width=16, state='readonly', values=['triangular_hp', 'rectangular', 'gaussian', 'lipshitz']).grid(row=r, column=1, padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Шум квантования при конвертации в MP3.")
        r += 1

        ttk.Checkbutton(f, text="ID3 Padding Attack", variable=self.v_id3pad, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_id3pad_val, 0, 2048, 64, width=6, fmt=None).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="байт").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Мусорные данные в тегах ID3v2.")
        r += 1

        for v in (self.v_phase_scr_val, self.v_resamp_val, self.v_ultra_level):
            v.trace_add('write', lambda *a: self._check_conflicts())

    def _build_texture_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Текстурные")

        r = 0
        ttk.Checkbutton(f, text="Спектральное маскирование", variable=self.v_spectral_mask, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        mask_frame = ttk.Frame(f)
        mask_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(mask_frame, text="Чувствительность:").pack(side='left')
        self._spin(mask_frame, self.v_spectral_mask_sens, 0.1, 2.0, 0.1, width=5).pack(side='left', padx=2)
        ttk.Label(mask_frame, text="  Аттенюация (dB):").pack(side='left')
        self._spin(mask_frame, self.v_spectral_mask_att, 1, 30, 1, width=4, fmt=None).pack(side='left', padx=2)
        ttk.Label(mask_frame, text="  Пиков:").pack(side='left')
        self._spin(mask_frame, self.v_spectral_mask_peaks, 1, 20, 1, width=4, fmt=None).pack(side='left', padx=2)
        r += 1

        ttk.Checkbutton(f, text="Эмуляция концертной записи", variable=self.v_concert_emu, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        concert_frame = ttk.Frame(f)
        concert_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(concert_frame, text="Интенсивность:").pack(side='left')
        ttk.Combobox(concert_frame, textvariable=self.v_concert_intensity, width=10, state='readonly', values=['light', 'medium', 'heavy']).pack(side='left', padx=4)
        r += 1

        ttk.Checkbutton(f, text="Mid/Side обработка", variable=self.v_midside, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        ms_frame = ttk.Frame(f)
        ms_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(ms_frame, text="Mid Gain (dB):").pack(side='left')
        self._spin(ms_frame, self.v_midside_mid, -12.0, 6.0, 0.5, width=5).pack(side='left', padx=2)
        ttk.Label(ms_frame, text="  Side Gain (dB):").pack(side='left')
        self._spin(ms_frame, self.v_midside_side, -6.0, 12.0, 0.5, width=5).pack(side='left', padx=2)
        r += 1

        ttk.Checkbutton(f, text="Психоакустический шум", variable=self.v_psycho_noise, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        psycho_frame = ttk.Frame(f)
        psycho_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(psycho_frame, text="Интенсивность:").pack(side='left')
        self._spin(psycho_frame, self.v_psycho_intensity, 0.0001, 0.01, 0.0001, width=8, fmt='%.4f').pack(side='left', padx=4)
        r += 1

        ttk.Checkbutton(f, text="Аналоговое насыщение", variable=self.v_saturation, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        sat_frame = ttk.Frame(f)
        sat_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(sat_frame, text="Drive:").pack(side='left')
        self._spin(sat_frame, self.v_saturation_drive, 1.0, 5.0, 0.1, width=5).pack(side='left', padx=2)
        ttk.Label(sat_frame, text="  Mix:").pack(side='left')
        self._spin(sat_frame, self.v_saturation_mix, 0.0, 1.0, 0.05, width=5).pack(side='left', padx=2)
        r += 1

        ttk.Checkbutton(f, text="Временной джиттер", variable=self.v_temp_jitter, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        tj_frame = ttk.Frame(f)
        tj_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(tj_frame, text="Интенсивность:").pack(side='left')
        self._spin(tj_frame, self.v_jitter_intensity, 0.0, 0.01, 0.0005, width=7, fmt='%.4f').pack(side='left', padx=2)
        ttk.Label(tj_frame, text="  Частота (Гц):").pack(side='left')
        self._spin(tj_frame, self.v_jitter_freq, 0.1, 10.0, 0.1, width=5).pack(side='left', padx=2)
        r += 1

        ttk.Checkbutton(f, text="Спектральный джиттер", variable=self.v_spec_jitter, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        sj_frame = ttk.Frame(f)
        sj_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(sj_frame, text="Кол-во провалов:").pack(side='left')
        self._spin(sj_frame, self.v_spec_jitter_count, 1, 15, 1, width=4, fmt=None).pack(side='left', padx=2)
        ttk.Label(sj_frame, text="  Аттенюация (dB):").pack(side='left')
        self._spin(sj_frame, self.v_spec_jitter_att, 3, 30, 1, width=4, fmt=None).pack(side='left', padx=2)
        r += 1

        ttk.Separator(f, orient='horizontal').grid(row=r, column=0, columnspan=5, sticky='ew', pady=6)
        r += 1

        ttk.Checkbutton(f, text="VK Инфразвук", variable=self.v_vk_infra, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        vk_frame = ttk.Frame(f)
        vk_frame.grid(row=r, column=0, columnspan=5, sticky='w', padx=20, pady=2)
        ttk.Label(vk_frame, text="Режим:").pack(side='left')
        ttk.Combobox(vk_frame, textvariable=self.v_vk_infra_mode, width=12, state='readonly', values=['simple', 'modulated', 'phase', 'harmonic', 'maximum']).pack(side='left', padx=2)
        ttk.Label(vk_frame, text="  Частота (Гц):").pack(side='left')
        self._spin(vk_frame, self.v_vk_infra_freq, 1.0, 25.0, 0.5, width=5).pack(side='left', padx=2)
        ttk.Label(vk_frame, text="  Амплитуда:").pack(side='left')
        self._spin(vk_frame, self.v_vk_infra_amplitude, 0.0, 1.0, 0.05, width=5).pack(side='left', padx=2)
        r += 1

        vk_frame2 = ttk.Frame(f)
        vk_frame2.grid(row=r, column=0, columnspan=5, sticky='w', padx=20, pady=2)
        ttk.Label(vk_frame2, text="Мод. частота:").pack(side='left')
        self._spin(vk_frame2, self.v_vk_infra_mod_freq, 0.01, 1.0, 0.01, width=5).pack(side='left', padx=2)
        ttk.Label(vk_frame2, text="  Глубина мод.:").pack(side='left')
        self._spin(vk_frame2, self.v_vk_infra_mod_depth, 0.0, 1.0, 0.05, width=5).pack(side='left', padx=2)
        ttk.Label(vk_frame2, text="  Фаза:").pack(side='left')
        self._spin(vk_frame2, self.v_vk_infra_phase_shift, 0.0, 6.28, 0.1, width=5).pack(side='left', padx=2)
        r += 1

        vk_frame3 = ttk.Frame(f)
        vk_frame3.grid(row=r, column=0, columnspan=5, sticky='w', padx=20, pady=2)
        ttk.Label(vk_frame3, text="Форма волны:").pack(side='left')
        ttk.Combobox(vk_frame3, textvariable=self.v_vk_infra_waveform, width=10, state='readonly', values=['sine', 'triangle', 'square']).pack(side='left', padx=2)
        ttk.Checkbutton(vk_frame3, text="Адаптивная амплитуда", variable=self.v_vk_infra_adaptive).pack(side='left', padx=6)
        r += 1

        vk_frame4 = ttk.Frame(f)
        vk_frame4.grid(row=r, column=0, columnspan=5, sticky='w', padx=20, pady=2)
        ttk.Label(vk_frame4, text="Гармоники:").pack(side='left')
        ttk.Label(vk_frame4, text="H2:").pack(side='left', padx=(4, 0))
        self._spin(vk_frame4, self.v_vk_infra_h1, 0.0, 0.5, 0.01, width=5).pack(side='left')
        ttk.Label(vk_frame4, text="H3:").pack(side='left', padx=(4, 0))
        self._spin(vk_frame4, self.v_vk_infra_h2, 0.0, 0.5, 0.01, width=5).pack(side='left')
        ttk.Label(vk_frame4, text="H4:").pack(side='left', padx=(4, 0))
        self._spin(vk_frame4, self.v_vk_infra_h3, 0.0, 0.5, 0.01, width=5).pack(side='left')
        r += 1
        self._desc(f, r, 0, "Подмешивает инфразвуковую синусоиду с различными режимами модуляции.")

    def _build_advanced_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Дополнительные")

        r = 0
        ttk.Checkbutton(f, text="Обрезать начало (сек)", variable=self.v_trim, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_trim_val, 0.0, 60.0, 0.5, width=5).grid(row=r, column=1, padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Удаляет указанное количество секунд с начала трека.")
        r += 1

        ttk.Checkbutton(f, text="Вырезать фрагмент", variable=self.v_cut, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        cut_frame = ttk.Frame(f)
        cut_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(cut_frame, text="Позиция (%):").pack(side='left')
        self._spin(cut_frame, self.v_cut_pos, 0, 100, 1, width=4, fmt=None).pack(side='left', padx=2)
        ttk.Label(cut_frame, text="  Длительность (сек):").pack(side='left')
        self._spin(cut_frame, self.v_cut_dur, 0.1, 30.0, 0.5, width=5).pack(side='left', padx=2)
        r += 1

        ttk.Checkbutton(f, text="Плавное затухание (сек)", variable=self.v_fade, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_fade_val, 0.5, 30.0, 0.5, width=5).grid(row=r, column=1, padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Плавное затухание громкости в конце трека.")
        r += 1

        ttk.Checkbutton(f, text="Сращивание треков", variable=self.v_merge, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        merge_frame = ttk.Frame(f)
        merge_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        self.entry_extra = ttk.Entry(merge_frame, textvariable=self.v_extra, width=40)
        self.entry_extra.pack(side='left', padx=(0, 4))
        ttk.Button(merge_frame, text="Выбрать трек", command=self._select_extra_track).pack(side='left')
        r += 1

        ttk.Checkbutton(f, text="Подмена длительности", variable=self.v_broken, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        dur_frame = ttk.Frame(f)
        dur_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=20, pady=2)
        ttk.Label(dur_frame, text="Тип:").pack(side='left')
        self.cmb_broken = ttk.Combobox(dur_frame, textvariable=self.v_broken_t, width=25, state='readonly',
                                       values=["0: Случайная большая длительность", "1: Случайная малая длительность", "2: Случайная средняя длительность", "3: Максимальная длительность"])
        self.cmb_broken.current(0)
        self.cmb_broken.pack(side='left', padx=4)
        r += 1
        self.v_merge.trace_add('write', lambda *a: self._check_conflicts())
        self.v_extra.trace_add('write', lambda *a: self._check_conflicts())

    def _build_technical_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Технические")

        r = 0
        ttk.Checkbutton(f, text="Рандомизация битрейта", variable=self.v_bitrate_j, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Случайно выбирает битрейт из {192, 224, 256, 320} kbps.")
        r += 1

        ttk.Checkbutton(f, text="Удаление заголовка Xing", variable=self.v_frame_sh, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Удаляет Xing/VBR заголовок, делая файл похожим на CBR.")
        r += 1

        ttk.Checkbutton(f, text="Мусор в поле comment (100-500 символов)", variable=self.v_fake_meta, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Добавляет случайную строку в поле comment для сбивания анализа.")
        r += 1

        ttk.Checkbutton(f, text="Переупорядочить ID3 теги", variable=self.v_reorder, command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Перезаписывает ID3v2 теги в порядке v2.3 стандарта.")
        r += 1

    def _build_system_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Системные")

        cpu_count = os.cpu_count() or 4
        r = 0
        ttk.Label(f, text="Параллельных потоков:", font=('', 9, 'bold')).grid(row=r, column=0, sticky='w', padx=4, pady=(8, 2))
        r += 1

        thread_frame = ttk.Frame(f)
        thread_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=4, pady=2)
        self._spin(thread_frame, self.v_max_workers, 1, min(16, cpu_count * 2), 1, width=5, fmt=None).pack(side='left', padx=2)
        ttk.Button(thread_frame, text=f"Авто ({cpu_count})", command=lambda: self.v_max_workers.set(cpu_count)).pack(side='left', padx=8)
        r += 1
        self._desc(f, r, 0, "Количество одновременно обрабатываемых файлов. Рекомендуется = числу ядер CPU.")
        r += 1

        ttk.Label(f, text="Задержка между запусками (сек):", font=('', 9)).grid(row=r, column=0, sticky='w', padx=4, pady=(8, 2))
        r += 1
        delay_frame = ttk.Frame(f)
        delay_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=4, pady=2)
        self._spin(delay_frame, self.v_thread_delay, 0.0, 5.0, 0.1, width=6).pack(side='left', padx=2)
        r += 1
        self._desc(f, r, 0, "Задержка перед запуском обработки каждого файла (полезно при пакетной обработке).")
        r += 1

        ttk.Separator(f, orient='horizontal').grid(row=r, column=0, columnspan=4, sticky='ew', pady=8)
        r += 1
        ttk.Label(f, text=f"Drag & Drop: {'доступен' if _DND_AVAILABLE else 'недоступен'}", font=('', 9)).grid(row=r, column=0, sticky='w', padx=4)
        r += 1
        ttk.Label(f, text="Горячие клавиши: Ctrl+O, Ctrl+A, Delete, Ctrl+S", font=('', 8), foreground='#555').grid(row=r, column=0, columnspan=4, sticky='w', padx=22)
        r += 1
        ttk.Label(f, text="Сохранение пресета: Ctrl+S (когда фокус не в текстовом поле)", font=('', 8), foreground='#555').grid(row=r, column=0, columnspan=4, sticky='w', padx=22)

    def _build_output_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Настройки вывода", padding=6)
        lf.pack(side='left', fill='both', expand=True, padx=(0, 4))

        dir_row = ttk.Frame(lf)
        dir_row.pack(fill='x', pady=2)
        ttk.Button(dir_row, text="Выбрать папку", command=self._select_output_dir).pack(side='left')
        self.lbl_out_dir = ttk.Label(dir_row, text=self.output_dir, relief='sunken', padding=2, width=30)
        self.lbl_out_dir.pack(side='left', padx=4, fill='x', expand=True)

        ttk.Checkbutton(lf, text="Сохранить оригинальные теги", variable=self.v_preserve_meta).pack(anchor='w')
        ttk.Checkbutton(lf, text="Сохранить оригинальную обложку", variable=self.v_preserve_cover).pack(anchor='w')
        ttk.Checkbutton(lf, text="Удалять оригиналы после обработки", variable=self.v_delete_orig).pack(anchor='w')

        q_frame = ttk.Frame(lf)
        q_frame.pack(fill='x', pady=(8, 2))
        ttk.Label(q_frame, text="Качество аудио:", font=('', 9, 'bold')).pack(side='left')
        self.cmb_quality = ttk.Combobox(q_frame, textvariable=self.v_quality, width=20, state='readonly',
                                         values=['320 kbps (CBR)', '245 kbps (VBR Q0)', '175 kbps (VBR Q4)', '130 kbps (VBR Q6)'])
        self.cmb_quality.pack(side='left', padx=4)
        ttk.Label(lf, text="320 kbps — макс. качество | 130 kbps — мин. размер", foreground='#888', font=('', 7)).pack(anchor='w', pady=(2, 0))

    def _show_template_help(self):
        help_win = tk.Toplevel(self.root)
        help_win.title("Помощь по шаблонам имён файлов")
        help_win.geometry("750x650")
        help_win.minsize(650, 550)
        help_win.transient(self.root)
        help_win.grab_set()

        main_frame = ttk.Frame(help_win, padding=15)
        main_frame.pack(fill='both', expand=True)

        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill='x', pady=(0, 15))
        ttk.Label(header_frame, text="📖 Документация по шаблонам имён файлов", font=('', 14, 'bold')).pack(side='left')

        text = tk.Text(main_frame, wrap='word', font=('Consolas', 10), padx=15, pady=15, bg='#f8f9fa')
        scroll = ttk.Scrollbar(main_frame, orient='vertical', command=text.yview)
        text.configure(yscrollcommand=scroll.set)
        scroll.pack(side='right', fill='y')
        text.pack(side='left', fill='both', expand=True)

        help_content = (
            "ДОСТУПНЫЕ ПЕРЕМЕННЫЕ\n"
            "{n} -> Номер, {n:03d} -> Номер 001, {original} -> Ориг. имя, {title}, {artist}, {album}, {year}\n"
            "ПРИМЕРЫ: {artist} - {title}, VK_{n:03d}_custom\n"
            "Ctrl+C/V/X/A работают во всех полях."
        )
        text.insert('1.0', help_content)
        text.config(state='disabled')

        help_win.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() - help_win.winfo_width()) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - help_win.winfo_height()) // 2
        help_win.geometry(f"+{x}+{y}")

    def _build_preset_management(self, parent):
        lf = ttk.LabelFrame(parent, text="Управление пресетами", padding=6)
        lf.pack(side='left', fill='both')

        name_row = ttk.Frame(lf)
        name_row.pack(fill='x', pady=2)
        ttk.Entry(name_row, textvariable=self.v_preset_name, width=18).pack(side='left')
        ttk.Button(name_row, text="Сохранить", command=self._save_preset).pack(side='left', padx=2)

        btn_row = ttk.Frame(lf)
        btn_row.pack(fill='x', pady=2)
        ttk.Button(btn_row, text="Экспорт", command=self._export_preset).pack(side='left', padx=2)
        ttk.Button(btn_row, text="Импорт", command=self._import_preset).pack(side='left', padx=2)

        sb2 = ttk.Scrollbar(lf, orient='vertical')
        self.preset_listbox = tk.Listbox(lf, yscrollcommand=sb2.set, height=5, width=28, exportselection=False)
        sb2.config(command=self.preset_listbox.yview)
        sb2.pack(side='right', fill='y')
        self.preset_listbox.pack(fill='both', expand=True)

        btns = ttk.Frame(lf)
        btns.pack(fill='x', pady=2)
        ttk.Button(btns, text="Загрузить", command=self._load_selected_preset).pack(side='left', padx=2)
        ttk.Button(btns, text="Удалить", command=self._delete_selected_preset).pack(side='left', padx=2)

        self._refresh_preset_list()

    def _build_action_section(self, parent):
        f = ttk.Frame(parent)
        f.pack(fill='x', padx=6, pady=4)

        self.progress_var = tk.IntVar()
        self.progress_bar = ttk.Progressbar(f, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(side='left', fill='x', expand=True, padx=(0, 8))

        self.btn_start = ttk.Button(f, text="Запустить обработку", command=self._start)
        self.btn_start.pack(side='right')

    def _build_log_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Лог", padding=4)
        lf.pack(fill='x', padx=6, pady=(0, 6))

        self.log_text = scrolledtext.ScrolledText(lf, height=7, state='disabled', font=('Courier', 9), wrap='word')
        self.log_text.pack(fill='both', expand=True)
        self.log_text.tag_config('info', foreground='#333333')
        self.log_text.tag_config('success', foreground='#007700')
        self.log_text.tag_config('warning', foreground='#aa6600')
        self.log_text.tag_config('error', foreground='#cc0000')

    def _safe_filename(self, s):
        import re
        safe = re.sub(r'[\\/*?:"<>|]', '_', str(s))
        safe = ' '.join(safe.split())
        return safe.strip() or '_'

    def _setup_hotkeys(self):
        self.root.bind('<Control-o>', lambda e: self._add_files_dialog())
        self.root.bind('<Control-O>', lambda e: self._add_files_dialog())
        self.root.bind('<Control-s>', lambda e: self._save_preset())
        self.root.bind('<Control-S>', lambda e: self._save_preset())

        self.file_listbox.bind('<Control-a>', self._listbox_select_all)
        self.file_listbox.bind('<Control-A>', self._listbox_select_all)
        self.file_listbox.bind('<Delete>', lambda e: self._remove_selected())
        self.file_listbox.bind('<Control-c>', self._listbox_copy)
        self.file_listbox.bind('<Control-C>', self._listbox_copy)

        # Дополнительные горячие клавиши для полей ввода
        for widget_class in ('TEntry', 'TCombobox', 'TSpinbox', 'Text', 'Listbox'):
            self.root.bind_class(widget_class, '<Control-c>', self._bind_copy)
            self.root.bind_class(widget_class, '<Control-C>', self._bind_copy)
            self.root.bind_class(widget_class, '<Control-v>', self._bind_paste)
            self.root.bind_class(widget_class, '<Control-V>', self._bind_paste)
            self.root.bind_class(widget_class, '<Control-x>', self._bind_cut)
            self.root.bind_class(widget_class, '<Control-X>', self._bind_cut)
            self.root.bind_class(widget_class, '<Control-a>', self._bind_select_all)
            self.root.bind_class(widget_class, '<Control-A>', self._bind_select_all)

    def _bind_copy(self, event):
        try:
            w = event.widget
            if hasattr(w, 'tag_ranges') and w.tag_ranges('sel'):
                text = w.get('sel.first', 'sel.last')
            elif hasattr(w, 'selection_get'):
                text = w.selection_get()
            else:
                return
            self.root.clipboard_clear()
            self.root.clipboard_append(text)
        except Exception:
            pass
        return 'break'

    def _bind_paste(self, event):
        try:
            w = event.widget
            text = self.root.clipboard_get()
            if hasattr(w, 'tag_ranges') and w.tag_ranges('sel'):
                w.delete('sel.first', 'sel.last')
            w.insert('insert', text)
        except Exception:
            pass
        return 'break'

    def _bind_cut(self, event):
        try:
            w = event.widget
            if hasattr(w, 'tag_ranges') and w.tag_ranges('sel'):
                text = w.get('sel.first', 'sel.last')
            elif hasattr(w, 'selection_get'):
                text = w.selection_get()
            else:
                return
            self.root.clipboard_clear()
            self.root.clipboard_append(text)
            if hasattr(w, 'delete'):
                if hasattr(w, 'tag_ranges') and w.tag_ranges('sel'):
                    w.delete('sel.first', 'sel.last')
                else:
                    w.delete('sel.first', 'sel.last')
        except Exception:
            pass
        return 'break'

    def _bind_select_all(self, event):
        try:
            w = event.widget
            if hasattr(w, 'tag_ranges'):
                w.tag_add('sel', '1.0', 'end')
            else:
                w.select_range(0, 'end')
                w.icursor('end')
        except Exception:
            pass
        return 'break'

    def _listbox_select_all(self, event=None):
        self.file_listbox.select_set(0, 'end')
        if self.input_files:
            self.btn_remove.config(state='normal')
        return 'break'

    def _listbox_copy(self, event=None):
        sel = self.file_listbox.curselection()
        if not sel:
            return 'break'
        names = '\n'.join(os.path.basename(self.input_files[i]) for i in sel)
        self.root.clipboard_clear()
        self.root.clipboard_append(names)
        return 'break'

    def _setup_drop_targets(self):
        if not _DND_AVAILABLE:
            return
        self.file_listbox.drop_target_register(_DND_FILES)
        self.file_listbox.dnd_bind('<<Drop>>', self._on_listbox_drop)
        self.lbl_cover.drop_target_register(_DND_FILES)
        self.lbl_cover.dnd_bind('<<Drop>>', self._on_cover_drop)
        if hasattr(self, 'entry_extra'):
            self.entry_extra.drop_target_register(_DND_FILES)
            self.entry_extra.dnd_bind('<<Drop>>', self._on_extra_drop)

    def _on_listbox_drop(self, event):
        files = self.root.tk.splitlist(event.data)
        audio_files = [f for f in files if any(f.lower().endswith(ext) for ext in SUPPORTED_FORMATS.keys())]
        if audio_files:
            self._add_files(audio_files)

    def _on_cover_drop(self, event):
        files = self.root.tk.splitlist(event.data)
        imgs = [f for f in files if os.path.splitext(f)[1].lower() in ('.png', '.jpg', '.jpeg')]
        if imgs:
            self._cleanup_temp_cover()
            self.selected_cover_path = imgs[0]
            self.lbl_cover.config(text=os.path.basename(imgs[0]))
            self.btn_rm_cover.config(state='normal')
            self._log(f"Обложка (DnD): {os.path.basename(imgs[0])}", 'success')

    def _on_extra_drop(self, event):
        files = self.root.tk.splitlist(event.data)
        audio_files = [f for f in files if any(f.lower().endswith(ext) for ext in SUPPORTED_FORMATS.keys())]
        if audio_files:
            self.v_extra.set(audio_files[0])

    def _check_ffmpeg(self):
        try:
            subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
            self.lbl_ffmpeg.config(text="FFmpeg найден")
            return True
        except Exception:
            self.lbl_ffmpeg.config(text="FFmpeg не найден", foreground='red')
            return False

    def _check_conflicts(self):
        warns = []
        if self.v_merge.get() and not self.v_extra.get().strip():
            warns.append("Merge: не указан дополнительный трек")
        if self.v_silence.get() and self.v_fade.get():
            warns.append("Тишина + Затухание: могут конфликтовать")
        if self.v_broken.get() and self.v_frame_sh.get():
            warns.append("Broken Duration + Frame Shift: могут сломать структуру MP3")
        if self.v_spectral_mask.get() and self.v_spec_jitter.get():
            warns.append("Spectral Masking + Spectral Jitter: множественные провалы")
        if self.v_concert_emu.get() and self.v_midside.get():
            warns.append("Concert Emulation + Mid/Side: избыточно")
        if self.v_vk_infra.get() and self.v_ultra.get():
            warns.append("VK Инфразвук + Ultrasonic: двойная обработка")
        if self.v_temp_jitter.get() and (self.v_pitch.get() or self.v_speed.get()):
            warns.append("Temporal Jitter + Pitch/Speed: множественные изменения времени")

        try:
            if self.v_vk_infra.get() and self.v_vk_infra_amplitude.get() > 0.5:
                warns.append(f"VK Инфразвук: амплитуда {self.v_vk_infra_amplitude.get():.2f} > 0.5")
            if self.v_ultra.get() and self.v_ultra_level.get() > 0.008:
                warns.append(f"Ultrasonic level {self.v_ultra_level.get():.4f} - высокий")
        except tk.TclError:
            pass

        if warns:
            self.lbl_conflict.config(text="\n".join(f"WARN: {w}" for w in warns))
        else:
            self.lbl_conflict.config(text="")

        self.btn_start.config(state='normal')
        self._schedule_preview_update()

    def _add_files_dialog(self):
        if self._mode == 'converter':
            files = filedialog.askopenfilenames(title="Выберите аудиофайлы", filetypes=INPUT_EXTENSIONS)
        else:
            files = filedialog.askopenfilenames(title="Выберите MP3 файлы", filetypes=[("MP3 files", "*.mp3")])
        if files:
            self._add_files(list(files))

    def _add_files(self, files):
        added = 0
        for fp in files:
            if fp not in self.input_files:
                self.input_files.append(fp)
                if self._mode == 'modifier':
                    self.tracks_info.append(TrackInfo(fp))
                else:
                    self.tracks_info.append(None)
                self.file_listbox.insert('end', os.path.basename(fp))
                added += 1
        if added:
            self._update_stats()
            self._log(f"Добавлено файлов: {added}", 'success')

    def _on_file_select(self, event):
        sel = self.file_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        self.current_index = idx
        self.btn_remove.config(state='normal')
        if self._mode == 'modifier':
            self._update_track_info(idx)
            self._load_waveform_for_file(self.input_files[idx])
            self._update_name_preview()
            self._live_preview_template()
        else:
            self._update_track_info(-1)
            self._clear_waveforms()

    def _remove_selected(self):
        if self.current_index < 0:
            return
        name = os.path.basename(self.input_files[self.current_index])
        self.input_files.pop(self.current_index)
        self.tracks_info.pop(self.current_index)
        self.file_listbox.delete(self.current_index)
        self.current_index = -1
        self.btn_remove.config(state='disabled')
        self._update_track_info(-1)
        self._update_stats()
        self._clear_waveforms()
        self._log(f"Удалён: {name}", 'warning')

    def _clear_files(self):
        self.input_files.clear()
        self.tracks_info.clear()
        self.file_listbox.delete(0, 'end')
        self.current_index = -1
        self.btn_remove.config(state='disabled')
        self._update_track_info(-1)
        self._update_stats()
        self._clear_waveforms()
        self._log("Список очищен", 'warning')

    def _on_pane_resize(self, event):
        if hasattr(self, 'canvas_before') and self._waveform_samples:
            self._schedule_redraw()

    def _update_stats(self):
        n = len(self.input_files)
        total_mb = sum(os.path.getsize(f) for f in self.input_files) / (1024 * 1024) if self.input_files else 0
        self.lbl_stats.config(text=f"{n} файлов | {total_mb:.1f} MB")

    def _update_track_info(self, index):
        self.txt_track_info.config(state='normal')
        self.txt_track_info.delete('1.0', 'end')
        if index < 0 or index >= len(self.tracks_info):
            self.txt_track_info.insert('end', "Файл не выбран")
        else:
            t = self.tracks_info[index]
            if t:
                dur = str(timedelta(seconds=int(t.duration_sec))) if t.duration_sec else '--:--'
                lines = [
                    f"Файл:    {t.file_name}",
                    f"Размер:  {t.size_mb:.2f} MB",
                    f"Длина:   {dur}",
                    f"Битрейт: {t.bitrate or '?'} kbps",
                    f"Частота: {t.sample_rate or '?'} Hz",
                    f"Название:{t.title or '(нет)'}",
                    f"Артист:  {t.artist or '(нет)'}",
                    f"Обложка: {'есть' if t.cover_data else 'нет'}"
                ]
                self.txt_track_info.insert('end', "\n".join(lines))
            else:
                self.txt_track_info.insert('end', "Информация недоступна (режим конвертера)")
        self.txt_track_info.config(state='disabled')

    def _cleanup_temp_cover(self):
        if self._cover_is_temp and self.selected_cover_path and os.path.exists(self.selected_cover_path):
            try:
                os.unlink(self.selected_cover_path)
            except Exception:
                pass
        self._cover_is_temp = False

    def _select_cover(self):
        fp = filedialog.askopenfilename(title="Выберите обложку", filetypes=[("Images", "*.png *.jpg *.jpeg")])
        if fp:
            self._cleanup_temp_cover()
            self.selected_cover_path = fp
            self.lbl_cover.config(text=os.path.basename(fp))
            self.btn_rm_cover.config(state='normal')
            self._log(f"Обложка: {os.path.basename(fp)}", 'success')

    def _random_cover(self):
        try:
            import struct, zlib
            r, g, b = random.randint(50, 200), random.randint(50, 200), random.randint(50, 200)
            def png_chunk(tag, data):
                c = zlib.crc32(tag + data) & 0xffffffff
                return struct.pack('>I', len(data)) + tag + data + struct.pack('>I', c)
            header = b'\x89PNG\r\n\x1a\n'
            ihdr = png_chunk(b'IHDR', struct.pack('>IIBBBBB', 16, 16, 8, 2, 0, 0, 0))
            row = bytes([0] + [r, g, b] * 16)
            idat = png_chunk(b'IDAT', zlib.compress(row * 16))
            iend = png_chunk(b'IEND', b'')
            png_data = header + ihdr + idat + iend
            self._cleanup_temp_cover()
            tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
            tmp.write(png_data)
            tmp.close()
            self.selected_cover_path = tmp.name
            self._cover_is_temp = True
            self.lbl_cover.config(text=f"Цвет #{r:02x}{g:02x}{b:02x}")
            self.btn_rm_cover.config(state='normal')
            self._log("Случайная обложка создана", 'success')
        except Exception as e:
            self._log(f"Ошибка генерации обложки: {e}", 'error')

    def _remove_cover(self):
        self._cleanup_temp_cover()
        self.selected_cover_path = None
        self.lbl_cover.config(text="(нет)")
        self.btn_rm_cover.config(state='disabled')
        self._log("Обложка удалена", 'info')

    def _copy_meta(self):
        if self.current_index < 0:
            messagebox.showinfo("Внимание", "Сначала выберите файл")
            return
        t = self.tracks_info[self.current_index]
        if t:
            self.v_title.set(t.title)
            self.v_artist.set(t.artist)
            self.v_album.set(t.album)
            self.v_year.set(t.year)
            self.v_genre.set(t.genre)
            self._log("Метаданные скопированы из оригинала", 'success')

    def _random_meta(self):
        self.v_title.set(f"Track {random.randint(1, 999)}")
        self.v_artist.set(f"Artist {random.randint(1, 99)}")
        self.v_album.set(f"Album {random.randint(2000, 2025)}")
        self.v_year.set(str(random.randint(2000, 2025)))
        self.v_genre.set(random.choice(["Pop", "Rock", "Electronic", "Hip Hop"]))
        self._log("Случайные метаданные", 'success')

    def _clear_meta(self):
        for v in (self.v_title, self.v_artist, self.v_album, self.v_year, self.v_genre):
            v.set('')

    def _select_output_dir(self):
        d = filedialog.askdirectory(title="Выберите папку для сохранения")
        if d:
            self.output_dir = d
            self.lbl_out_dir.config(text=d)
            if hasattr(self, 'lbl_out_dir_conv'):
                self.lbl_out_dir_conv.config(text=d)
            self._log(f"Папка вывода: {d}", 'success')

    def _select_extra_track(self):
        fp = filedialog.askopenfilename(title="Дополнительный трек", filetypes=INPUT_EXTENSIONS)
        if fp:
            self.v_extra.set(fp)

    def _save_preset(self):
        name = self.v_preset_name.get().strip() or f"Preset {len(self.saved_presets)+1}"
        data = {'name': name, 'date': datetime.now().isoformat(), 'settings': self._collect_settings()}
        self.saved_presets.append(data)
        self._refresh_preset_list()
        self.v_preset_name.set('')
        self._save_config()
        self._log(f"Пресет '{name}' сохранён", 'success')

    def _refresh_preset_list(self):
        self.preset_listbox.delete(0, 'end')
        for p in self.saved_presets:
            self.preset_listbox.insert('end', p.get('name', '?'))

    def _load_selected_preset(self):
        sel = self.preset_listbox.curselection()
        if not sel:
            return
        data = self.saved_presets[sel[0]]
        s = data.get('settings', {})
        if not s:
            self._log(f"Пресет '{data['name']}' не содержит настроек", 'warning')
            return

        methods = s.get('methods', {})
        self.v_pitch.set(methods.get('pitch', False))
        self.v_speed.set(methods.get('speed', False))
        self.v_eq.set(methods.get('eq', False))
        self.v_silence.set(methods.get('silence', False))
        self.v_phase_inv.set(methods.get('phase_invert', False))
        self.v_phase_scr.set(methods.get('phase_scramble', False))
        self.v_dc.set(methods.get('dc_shift', False))
        self.v_resamp.set(methods.get('resample_drift', False))
        self.v_ultra.set(methods.get('ultrasonic_noise', False))
        self.v_haas.set(methods.get('haas_delay', False))
        self.v_dither.set(methods.get('dither_attack', False))
        self.v_id3pad.set(methods.get('id3_padding_attack', False))
        self.v_trim.set(methods.get('trim_silence', False))
        self.v_cut.set(methods.get('cut_fragment', False))
        self.v_fade.set(methods.get('fade_out', False))
        self.v_merge.set(methods.get('merge', False))
        self.v_broken.set(methods.get('broken_duration', False))
        self.v_bitrate_j.set(methods.get('bitrate_jitter', False))
        self.v_frame_sh.set(methods.get('frame_shift', False))
        self.v_fake_meta.set(methods.get('fake_metadata', False))
        self.v_reorder.set(methods.get('reorder_tags', False))
        self.v_spectral_mask.set(methods.get('spectral_masking', False))
        self.v_concert_emu.set(methods.get('concert_emulation', False))
        self.v_midside.set(methods.get('midside_processing', False))
        self.v_psycho_noise.set(methods.get('psychoacoustic_noise', False))
        self.v_saturation.set(methods.get('saturation', False))
        self.v_temp_jitter.set(methods.get('temporal_jitter', False))
        self.v_spec_jitter.set(methods.get('spectral_jitter', False))
        self.v_vk_infra.set(methods.get('vk_infrasonic', False))

        self.v_filename_template.set(s.get('filename_template', 'VK_{n:03d}_custom'))
        self._check_conflicts()
        self._log(f"Загружен пресет: {data['name']}", 'success')

    def _delete_selected_preset(self):
        sel = self.preset_listbox.curselection()
        if not sel:
            return
        name = self.saved_presets[sel[0]]['name']
        self.saved_presets.pop(sel[0])
        self._refresh_preset_list()
        self._save_config()
        self._log(f"Пресет '{name}' удалён", 'warning')

    def _export_preset(self):
        fp = filedialog.asksaveasfilename(defaultextension='.json', filetypes=[("JSON", "*.json")])
        if fp:
            try:
                with open(fp, 'w', encoding='utf-8') as f:
                    json.dump(self.saved_presets, f, indent=2, ensure_ascii=False)
                self._log(f"Пресеты экспортированы: {os.path.basename(fp)}", 'success')
            except Exception as e:
                self._log(f"Ошибка экспорта: {e}", 'error')

    def _import_preset(self):
        fp = filedialog.askopenfilename(filetypes=[("JSON", "*.json")])
        if fp:
            try:
                with open(fp, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    self.saved_presets.extend(data)
                else:
                    self.saved_presets.append(data)
                self._refresh_preset_list()
                self._save_config()
                self._log(f"Пресеты импортированы из {os.path.basename(fp)}", 'success')
            except Exception as e:
                self._log(f"Ошибка импорта: {e}", 'error')

    def _collect_settings(self):
        eq_type_idx = self.cmb_eq_type.current() if hasattr(self, 'cmb_eq_type') else 0
        broken_type_idx = self.cmb_broken.current() if hasattr(self, 'cmb_broken') else 0
        quality_map = ['320k', '0', '4', '6']
        q_idx = 0
        try:
            q_vals = ['320 kbps (CBR)', '245 kbps (VBR Q0)', '175 kbps (VBR Q4)', '130 kbps (VBR Q6)']
            q_idx = q_vals.index(self.v_quality.get())
        except ValueError:
            pass

        return {
            'methods': {
                'pitch': self.v_pitch.get(), 'speed': self.v_speed.get(), 'eq': self.v_eq.get(), 'silence': self.v_silence.get(),
                'phase_invert': self.v_phase_inv.get(), 'phase_scramble': self.v_phase_scr.get(), 'dc_shift': self.v_dc.get(),
                'resample_drift': self.v_resamp.get(), 'ultrasonic_noise': self.v_ultra.get(), 'haas_delay': self.v_haas.get(),
                'dither_attack': self.v_dither.get(), 'id3_padding_attack': self.v_id3pad.get(), 'trim_silence': self.v_trim.get(),
                'cut_fragment': self.v_cut.get(), 'fade_out': self.v_fade.get(), 'merge': self.v_merge.get(),
                'broken_duration': self.v_broken.get(), 'bitrate_jitter': self.v_bitrate_j.get(), 'frame_shift': self.v_frame_sh.get(),
                'fake_metadata': self.v_fake_meta.get(), 'reorder_tags': self.v_reorder.get(), 'spectral_masking': self.v_spectral_mask.get(),
                'concert_emulation': self.v_concert_emu.get(), 'midside_processing': self.v_midside.get(),
                'psychoacoustic_noise': self.v_psycho_noise.get(), 'saturation': self.v_saturation.get(),
                'temporal_jitter': self.v_temp_jitter.get(), 'spectral_jitter': self.v_spec_jitter.get(), 'vk_infrasonic': self.v_vk_infra.get()
            },
            'filename_template': self.v_filename_template.get() or 'VK_{n:03d}_custom',
            'quality': quality_map[q_idx],
            'rename_files': self.v_rename.get(),
            'preserve_metadata': self.v_preserve_meta.get()
        }

    def _start(self):
        if not self.input_files:
            messagebox.showwarning("Внимание", "Добавьте MP3 файлы")
            return
        if not self.output_dir:
            messagebox.showwarning("Внимание", "Выберите папку для сохранения")
            return

        os.makedirs(self.output_dir, exist_ok=True)
        self.btn_start.config(state='disabled')
        self.progress_var.set(0)
        self.progress_bar.config(maximum=len(self.input_files))
        self._completed_count = 0

        settings = self._collect_settings()
        metadata = {
            'title': self.v_title.get(), 'artist': self.v_artist.get(),
            'album': self.v_album.get(), 'year': self.v_year.get(), 'genre': self.v_genre.get()
        }

        max_workers = self.v_max_workers.get()
        self._log(f"Запущена обработка ({max_workers} поток(а))...", 'info')

        if max_workers > 1:
            processor = BatchProcessor(
                files=list(self.input_files), tracks_info=list(self.tracks_info),
                output_dir=self.output_dir, settings=settings, metadata=metadata,
                result_queue=self._worker_queue, max_workers=max_workers,
                delay_between=self.v_thread_delay.get()
            )
            processor.run_in_thread()
        else:
            worker = ModificationWorker(
                files=list(self.input_files), tracks_info=list(self.tracks_info),
                output_dir=self.output_dir, settings=settings, metadata=metadata,
                on_progress=lambda cur, tot, fp: self._worker_queue.put(('progress', cur, tot, fp)),
                on_file_complete=lambda fp, ok, out: self._worker_queue.put(('file_done', fp, ok, out)),
                on_all_complete=lambda sc, tot: self._worker_queue.put(('all_done', sc, tot)),
                on_error=lambda msg: self._worker_queue.put(('error', msg))
            )
            worker.start()
        self._poll_queue()

    def _poll_queue(self):
        try:
            while True:
                msg = self._worker_queue.get_nowait()
                kind = msg[0]

                if kind == 'progress':
                    _, cur, tot, fp = msg
                    self._log(f"[{cur}/{tot}] {os.path.basename(fp)}", 'info')
                elif kind == 'file_done':
                    _, fp, ok, out = msg
                    self._completed_count += 1
                    self.progress_var.set(self._completed_count)
                    if ok:
                        self._log(f"OK {os.path.basename(fp)} -> {os.path.basename(out)}", 'success')
                    else:
                        self._log(f"ERROR {os.path.basename(fp)}", 'error')
                elif kind == 'all_done':
                    _, sc, tot = msg
                    self.btn_start.config(state='normal')
                    self._check_conflicts()
                    self._log(f"Готово: {sc}/{tot} файлов обработано", 'success')
                    messagebox.showinfo("Готово", f"Обработано {sc} из {tot} файлов")
                    return
                elif kind == 'error':
                    self._log(f"ERROR: {msg[1]}", 'error')
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)

    def _get_all_settings_vars(self):
        """Возвращает словарь со всеми настраиваемыми переменными для сохранения/загрузки"""
        return {
            # Методы обработки
            'pitch_enabled': self.v_pitch,
            'pitch_value': self.v_pitch_val,
            'speed_enabled': self.v_speed,
            'speed_value': self.v_speed_val,
            'eq_enabled': self.v_eq,
            'eq_type': self.v_eq_type,
            'eq_value': self.v_eq_val,
            'silence_enabled': self.v_silence,
            'silence_value': self.v_silence_val,
            'phase_inv_enabled': self.v_phase_inv,
            'phase_inv_value': self.v_phase_inv_val,
            'phase_scr_enabled': self.v_phase_scr,
            'phase_scr_value': self.v_phase_scr_val,
            'dc_enabled': self.v_dc,
            'dc_value': self.v_dc_val,
            'resamp_enabled': self.v_resamp,
            'resamp_value': self.v_resamp_val,
            'ultra_enabled': self.v_ultra,
            'ultra_freq': self.v_ultra_freq,
            'ultra_level': self.v_ultra_level,
            'haas_enabled': self.v_haas,
            'haas_value': self.v_haas_val,
            'dither_enabled': self.v_dither,
            'dither_method': self.v_dither_method,
            'id3pad_enabled': self.v_id3pad,
            'id3pad_value': self.v_id3pad_val,
            # Дополнительные методы
            'spectral_mask_enabled': self.v_spectral_mask,
            'spectral_mask_sens': self.v_spectral_mask_sens,
            'spectral_mask_att': self.v_spectral_mask_att,
            'spectral_mask_peaks': self.v_spectral_mask_peaks,
            'concert_emu_enabled': self.v_concert_emu,
            'concert_intensity': self.v_concert_intensity,
            'midside_enabled': self.v_midside,
            'midside_mid': self.v_midside_mid,
            'midside_side': self.v_midside_side,
            'psycho_noise_enabled': self.v_psycho_noise,
            'psycho_intensity': self.v_psycho_intensity,
            'saturation_enabled': self.v_saturation,
            'saturation_drive': self.v_saturation_drive,
            'saturation_mix': self.v_saturation_mix,
            'temp_jitter_enabled': self.v_temp_jitter,
            'jitter_intensity': self.v_jitter_intensity,
            'jitter_freq': self.v_jitter_freq,
            'spec_jitter_enabled': self.v_spec_jitter,
            'spec_jitter_count': self.v_spec_jitter_count,
            'spec_jitter_att': self.v_spec_jitter_att,
            # Инфразвуковой генератор
            'vk_infra_enabled': self.v_vk_infra,
            'vk_infra_mode': self.v_vk_infra_mode,
            'vk_infra_amplitude': self.v_vk_infra_amplitude,
            'vk_infra_freq': self.v_vk_infra_freq,
            'vk_infra_mod_freq': self.v_vk_infra_mod_freq,
            'vk_infra_mod_depth': self.v_vk_infra_mod_depth,
            'vk_infra_phase_shift': self.v_vk_infra_phase_shift,
            'vk_infra_waveform': self.v_vk_infra_waveform,
            'vk_infra_adaptive': self.v_vk_infra_adaptive,
            'vk_infra_h1': self.v_vk_infra_h1,
            'vk_infra_h2': self.v_vk_infra_h2,
            'vk_infra_h3': self.v_vk_infra_h3,
            # Обрезка и фрагменты
            'trim_enabled': self.v_trim,
            'trim_value': self.v_trim_val,
            'cut_enabled': self.v_cut,
            'cut_pos': self.v_cut_pos,
            'cut_dur': self.v_cut_dur,
            'fade_enabled': self.v_fade,
            'fade_value': self.v_fade_val,
            'merge_enabled': self.v_merge,
            'extra_file': self.v_extra,
            'broken_enabled': self.v_broken,
            'broken_time': self.v_broken_t,
            # Специальные опции
            'bitrate_jitter': self.v_bitrate_j,
            'frame_shift': self.v_frame_sh,
            'fake_meta': self.v_fake_meta,
            'reorder': self.v_reorder,
            # Метаданные и вывод
            'preserve_meta': self.v_preserve_meta,
            'preserve_cover': self.v_preserve_cover,
            'rename': self.v_rename,
            'delete_orig': self.v_delete_orig,
            'quality': self.v_quality,
            'title': self.v_title,
            'artist': self.v_artist,
            'album': self.v_album,
            'year': self.v_year,
            'genre': self.v_genre,
            'filename_template': self.v_filename_template,
            # Производительность
            'max_workers': self.v_max_workers,
            'thread_delay': self.v_thread_delay,
            # Конвертер
            'conv_format': self.v_conv_format,
            'conv_quality': self.v_conv_quality,
            'conv_delete': self.v_conv_delete,
        }

    def _load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                self.output_dir = cfg.get('output_dir', self.output_dir) or self.output_dir
                self.saved_presets = cfg.get('presets', [])
                self.user_templates = cfg.get('user_templates', [])
                if not self.user_templates:
                    self.user_templates = [{'name': f'Default {i+1}', 'pattern': p} for i, p in enumerate(DEFAULT_TEMPLATES)]
                
                # Загружаем все настройки
                settings = cfg.get('settings', {})
                all_vars = self._get_all_settings_vars()
                for key, var in all_vars.items():
                    if key in settings:
                        value = settings[key]
                        if isinstance(var, tk.BooleanVar):
                            var.set(bool(value))
                        elif isinstance(var, tk.IntVar):
                            var.set(int(value))
                        elif isinstance(var, tk.DoubleVar):
                            var.set(float(value))
                        elif isinstance(var, tk.StringVar):
                            var.set(str(value) if value is not None else '')
        except Exception:
            self.user_templates = [{'name': f'Default {i+1}', 'pattern': p} for i, p in enumerate(DEFAULT_TEMPLATES)]

    def _save_config(self):
        try:
            # Собираем все настройки
            settings = {}
            all_vars = self._get_all_settings_vars()
            for key, var in all_vars.items():
                settings[key] = var.get()
            
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'output_dir': self.output_dir,
                    'presets': self.saved_presets,
                    'user_templates': self.user_templates,
                    'settings': settings
                }, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def _log(self, message, level='info', to_converter=False):
        ts = datetime.now().strftime('%H:%M:%S')
        if to_converter:
            self.conv_log_text.config(state='normal')
            self.conv_log_text.insert('end', f"[{ts}] {message}\n", level)
            self.conv_log_text.see('end')
            self.conv_log_text.config(state='disabled')
        else:
            self.log_text.config(state='normal')
            self.log_text.insert('end', f"[{ts}] {message}\n", level)
            self.log_text.see('end')
            self.log_text.config(state='disabled')

    def _method_row(self, parent, row, text, var, *extra_widgets):
        cb = ttk.Checkbutton(parent, text=text, variable=var, command=self._check_conflicts)
        cb.grid(row=row, column=0, sticky='w', padx=4, pady=2)
        for col, w in enumerate(extra_widgets, start=1):
            w.grid(row=row, column=col, padx=4, pady=2, sticky='w')
        return cb

    def _spin(self, parent, var, from_, to, inc, width=8, fmt='%.2f'):
        return ttk.Spinbox(parent, textvariable=var, from_=from_, to=to, increment=inc, width=width,
                           format=fmt if isinstance(var, tk.DoubleVar) else None)

    def _desc(self, parent, row, col, text, colspan=4):
        ttk.Label(parent, text=text, foreground='gray', font=('', 8, 'italic')).grid(row=row, column=col,
                                                                                      columnspan=colspan, sticky='w',
                                                                                      padx=22, pady=(0, 3))

    def _live_preview_template(self):
        tpl = self._get_text_template_content()
        if not tpl.strip():
            self.lbl_template_live_preview.config(text="Предпросмотр: --")
            return

        if self.current_index >= 0 and self.current_index < len(self.tracks_info):
            ti = self.tracks_info[self.current_index]
            orig = os.path.splitext(os.path.basename(self.input_files[self.current_index]))[0]
            ex_title = self.v_title.get() or ti.title or orig
            ex_artist = self.v_artist.get() or ti.artist or ''
            ex_album = self.v_album.get() or ti.album or ''
            ex_year = self.v_year.get() or ti.year or ''
            n_val = self.current_index + 1
        else:
            orig = 'example_track'
            ex_title = 'Example Song'
            ex_artist = 'Example Artist'
            ex_album = 'Example Album'
            ex_year = '2024'
            n_val = 1

        try:
            fname = tpl.format(
                n=n_val, original=self._safe_filename(orig), title=self._safe_filename(ex_title),
                artist=self._safe_filename(ex_artist), album=self._safe_filename(ex_album),
                year=self._safe_filename(str(ex_year))
            ) + '.mp3'
            self.lbl_template_live_preview.config(text=f"Предпросмотр: {fname}", foreground='#007700')
        except (KeyError, ValueError) as e:
            self.lbl_template_live_preview.config(text=f"Ошибка: {e}", foreground='#cc0000')


class BatchProcessor:
    def __init__(self, files, tracks_info, output_dir, settings, metadata,
                 result_queue, max_workers=4, delay_between=0.0):
        self.files = files
        self.tracks_info = tracks_info
        self.output_dir = output_dir
        self.settings = settings
        self.metadata = metadata
        self.queue = result_queue
        self.max_workers = max_workers
        self.delay_between = delay_between
        self._success_count = 0
        self._lock = threading.Lock()

    def run_in_thread(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _process_one(self, idx, file_path, track_info):
        import time
        if self.delay_between > 0:
            time.sleep(self.delay_between * (idx % self.max_workers))

        total = len(self.files)
        self.queue.put(('progress', idx + 1, total, file_path))

        def _on_done(fp, ok, out):
            with self._lock:
                if ok:
                    self._success_count += 1
            self.queue.put(('file_done', fp, ok, out))

        worker = ModificationWorker(
            files=[file_path], tracks_info=[track_info],
            output_dir=self.output_dir, settings=self.settings, metadata=self.metadata,
            on_progress=lambda *a: None,
            on_file_complete=_on_done,
            on_all_complete=lambda *a: None,
            on_error=lambda msg: self.queue.put(('error', msg)),
            start_index=idx
        )
        worker.run()

    def _run(self):
        total = len(self.files)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_one, i, fp, ti): i
                for i, (fp, ti) in enumerate(zip(self.files, self.tracks_info))
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.queue.put(('error', str(e)))
        self.queue.put(('all_done', self._success_count, total))


def _compute_preview_static(samples, s):
    import math
    result = list(samples)
    sr = 500
    n = len(result)
    if n == 0:
        return result

    def _resample(data, factor):
        src_n = len(data)
        dst_n = max(1, int(src_n / factor))
        out = []
        for i in range(dst_n):
            src = i * factor
            i0 = int(src)
            frac = src - i0
            if i0 + 1 < src_n:
                out.append(data[i0] * (1 - frac) + data[i0 + 1] * frac)
            elif i0 < src_n:
                out.append(data[i0])
        return out

    if s.get('trim', False):
        cut = int(s.get('trim_val', 5.0) * sr)
        result = result[min(cut, n):]
        n = len(result)

    if s.get('cut', False) and n > 0:
        pos = s.get('cut_pos', 50) / 100.0
        dur = s.get('cut_dur', 2.0)
        cut_c = int(pos * n)
        cut_d = int(dur * sr)
        cut_s = max(0, cut_c - cut_d // 2)
        cut_e = min(n, cut_s + cut_d)
        result = result[:cut_s] + result[cut_e:]
        n = len(result)

    if s.get('speed', False) and n > 0:
        factor = s.get('speed_val', 1.0)
        if factor != 1.0 and factor > 0:
            result = _resample(result, factor)
            n = len(result)

    if s.get('pitch', False) and n > 0:
        semitones = s.get('pitch_val', 0.5)
        factor = 2 ** (semitones / 12)
        if factor != 1.0:
            result = _resample(result, factor)
            n = len(result)

    if s.get('resamp', False) and n > 0:
        drift = s.get('resamp_val', 1)
        factor = (44100 + drift) / 44100
        if factor != 1.0:
            resampled = _resample(result, factor)
            result = resampled[:n] + result[len(resampled):]
            n = len(result)

    if s.get('vk_infra', False) and n > 0:
        amp = s.get('vk_infra_amp', 0.35)
        freq = s.get('vk_infra_freq', 18.0)
        mode = s.get('vk_infra_mode', 'modulated')
        mod_freq = s.get('vk_infra_mod_freq', 0.08)
        mod_depth = s.get('vk_infra_mod_depth', 0.3)
        phase = s.get('vk_infra_phase', 0.0)
        waveform = s.get('vk_infra_waveform', 'sine')
        harmonics = s.get('vk_infra_harmonics', [0.15, 0.07, 0.03])

        def wave_func(wtype, arg):
            if wtype == 'sine':
                return math.sin(arg)
            elif wtype == 'triangle':
                return 2 / math.pi * math.asin(math.sin(arg))
            elif wtype == 'square':
                sval = math.sin(arg)
                return sval / (abs(sval) + 0.000001)
            return math.sin(arg)

        for i in range(n):
            t = i / sr
            base_arg = 2 * math.pi * freq * t + phase
            if mode == 'simple':
                wave = amp * wave_func(waveform, base_arg)
            elif mode == 'modulated':
                mod = (1 - mod_depth + mod_depth * math.sin(2 * math.pi * mod_freq * t))
                wave = amp * mod * wave_func(waveform, base_arg)
            elif mode == 'phase':
                phase_mod = mod_depth * math.sin(2 * math.pi * mod_freq * t)
                wave = amp * wave_func(waveform, base_arg + phase_mod)
            elif mode == 'harmonic':
                wave = amp * wave_func(waveform, base_arg)
                for idx, h_amp in enumerate(harmonics, start=2):
                    if h_amp > 0:
                        h_arg = 2 * math.pi * (freq * idx) * t + phase
                        wave += amp * h_amp * wave_func(waveform, h_arg)
            else:
                mod = (1 - mod_depth + mod_depth * math.sin(2 * math.pi * mod_freq * t))
                wave = amp * mod * wave_func(waveform, base_arg)
                if harmonics and harmonics[0] > 0:
                    h2_arg = 2 * math.pi * (freq * 2) * t + phase
                    wave += amp * harmonics[0] * mod * wave_func(waveform, h2_arg)
            result[i] = result[i] + wave

    if s.get('dc', False):
        offset = s.get('dc_val', 0.000005)
        result = [v + offset for v in result]

    if s.get('phase_inv', False):
        strength = s.get('phase_inv_val', 1.0)
        result = [v * (1.0 - 2.0 * strength) for v in result]

    if s.get('phase_scr', False) and n > 0:
        speed = s.get('phase_scr_val', 2.0)
        result = [result[i] * (1.0 + 0.15 * math.sin(2 * math.pi * speed * i / sr)) for i in range(n)]

    if s.get('haas', False) and n > 0:
        delay_ms = s.get('haas_val', 15.0)
        delay_samples = int(delay_ms / 1000.0 * sr)
        echo_gain = 0.3
        result2 = list(result)
        for i in range(delay_samples, n):
            result2[i] += result[i - delay_samples] * echo_gain
        result = result2

    if s.get('eq', False):
        eq_type = s.get('eq_type', 0)
        eq_val = s.get('eq_val', -2.0)
        gain = 10 ** (eq_val / 20)
        result = [v * gain for v in result]

    if s.get('saturation', False):
        drive = s.get('sat_drive', 1.5)
        mix = s.get('sat_mix', 0.15)
        result = [v * (1 - mix) + math.tanh(v * drive) * mix for v in result]

    if s.get('temp_jitter', False) and n > 0:
        intensity = s.get('jitter_intensity', 0.002)
        jfreq = s.get('jitter_freq', 0.5)
        warped = []
        t_acc = 0.0
        for i in range(n):
            t = i / sr
            speed_mod = 1.0 + intensity * math.sin(2 * math.pi * jfreq * t)
            i0 = int(t_acc)
            frac = t_acc - i0
            if i0 + 1 < n:
                warped.append(result[i0] * (1 - frac) + result[i0 + 1] * frac)
            elif i0 < n:
                warped.append(result[i0])
            else:
                warped.append(0.0)
            t_acc += speed_mod
            if t_acc >= n:
                break
        pad = n - len(warped)
        result = warped + [0.0] * pad
        n = len(result)

    if s.get('spec_jitter', False) and n > 0:
        count = s.get('spec_jitter_count', 5)
        att = s.get('spec_jitter_att', 15)
        floor = 10 ** (-att / 20)
        dip_w = max(1, n // (count * 6))
        positions = [int(n * (i + 0.5) / (count + 1)) for i in range(count)]
        for pos in positions:
            for j in range(max(0, pos - dip_w), min(n, pos + dip_w)):
                dist = abs(j - pos) / dip_w
                local_gain = floor + (1.0 - floor) * min(1.0, dist)
                result[j] *= local_gain

    if s.get('spectral_mask', False):
        att = s.get('spectral_mask_att', 12)
        peaks = s.get('spectral_mask_peaks', 10)
        reduction = 1.0 - (peaks / 27.0) * (att / 40.0) * 0.25
        result = [v * reduction for v in result]

    if s.get('concert', False) and n > 0:
        intensity = s.get('concert_intensity', 'medium')
        echo_g = {'light': 0.08, 'medium': 0.13, 'heavy': 0.20}.get(intensity, 0.13)
        echo_d = int({'light': 0.05, 'medium': 0.09, 'heavy': 0.14}.get(intensity, 0.09) * sr)
        result2 = list(result)
        for i in range(echo_d, n):
            result2[i] += result[i - echo_d] * echo_g
        result = result2

    if s.get('midside', False):
        mid_g = 10 ** (s.get('midside_mid', -3.0) / 40)
        side_g = 10 ** (s.get('midside_side', 2.0) / 40)
        blend = (mid_g + side_g) / 2
        result = [v * blend for v in result]

    if s.get('psycho', False) and n > 0:
        intensity = s.get('psycho_intensity', 0.0003)
        result = [result[i] + intensity * math.sin(i * 7.3 + math.cos(i * 3.7)) for i in range(n)]

    if s.get('ultra', False) and n > 0:
        level = s.get('ultra_level', 0.001)
        ultra_preview_freq = 80.0
        result = [result[i] + level * math.sin(2 * math.pi * ultra_preview_freq * i / sr) for i in range(n)]

    if s.get('silence', False):
        pad = int(s.get('silence_val', 45) * sr)
        result = result + [0.0] * pad
        n = len(result)

    if s.get('fade', False) and n > 0:
        fade_dur = s.get('fade_val', 5.0)
        total_dur = n / sr
        if 0 < fade_dur < total_dur:
            fade_start_i = int(n * (1 - fade_dur / total_dur))
            span = max(1, n - fade_start_i)
            for i in range(fade_start_i, n):
                result[i] *= 1.0 - (i - fade_start_i) / span

    return result


def main():
    if _DND_AVAILABLE:
        from tkinterdnd2 import TkinterDnD
        root = TkinterDnD.Tk()
    else:
        root = tk.Tk()
    app = VKModifierApp(root)
    root.mainloop()


if __name__ == '__main__':
    main()