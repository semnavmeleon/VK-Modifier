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
    ("Все аудио файлы", "*.mp3 *.flac *.wav *.ogg *.aac *.m4a *.wma *.opus *.aiff *.alac *.wv *.ape *.tta *.ac3 *.dts *.mp2 *.mpc *.spx *.amr *.au *.mka *.oga *.caf *.shn"),
    ("MP3 files", "*.mp3"),
    ("FLAC files", "*.flac"),
    ("WAV files", "*.wav"),
    ("OGG files", "*.ogg"),
    ("AAC files", "*.aac"),
    ("M4A files", "*.m4a"),
    ("WMA files", "*.wma"),
    ("Opus files", "*.opus"),
    ("AIFF files", "*.aiff"),
    ("ALAC files", "*.alac"),
    ("WavPack files", "*.wv"),
    ("Monkey's Audio files", "*.ape"),
    ("True Audio files", "*.tta"),
    ("AC3 files", "*.ac3"),
    ("DTS files", "*.dts"),
    ("MP2 files", "*.mp2"),
    ("Musepack files", "*.mpc"),
    ("Speex files", "*.spx"),
    ("AMR files", "*.amr"),
    ("AU files", "*.au"),
    ("Matroska Audio files", "*.mka"),
    ("Ogg FLAC files", "*.oga"),
    ("CAF files", "*.caf"),
    ("Shorten files", "*.shn"),
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
    'mp3': [
        '320 kbps (CBR)',
        '256 kbps (CBR)',
        '192 kbps (CBR)',
        '128 kbps (CBR)',
        'VBR Высшее (Q0)',
        'VBR Высокое (Q2)',
        'VBR Среднее (Q4)',
        'VBR Низкое (Q6)',
    ],
    'aac': [
        '320 kbps',
        '256 kbps',
        '192 kbps',
        '128 kbps',
    ],
    'm4a': [
        '320 kbps',
        '256 kbps',
        '192 kbps',
        '128 kbps',
    ],
    'ogg': [
        'Качество 10 (макс)',
        'Качество 8 (высокое)',
        'Качество 6 (среднее)',
        'Качество 4 (низкое)',
        'Качество 2 (мин)',
    ],
    'opus': [
        '256 kbps',
        '192 kbps',
        '128 kbps',
        '96 kbps',
        '64 kbps',
    ],
    'wma': [
        '320 kbps',
        '256 kbps',
        '192 kbps',
        '128 kbps',
    ],
}


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
        
        elif self.output_format in ['aac', 'm4a']:
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
        
        elif self.output_format == 'opus':
            bitrate = self.quality_preset.split()[0]
            args.extend(['-codec:a', codec, '-b:a', f'{bitrate}k'])
        
        elif self.output_format == 'wma':
            bitrate = self.quality_preset.split()[0]
            args.extend(['-codec:a', codec, '-b:a', f'{bitrate}k'])
        
        elif self.output_format == 'flac':
            if 'Compression' in self.quality_preset:
                comp = self.quality_preset.split()[-1]
                args.extend(['-codec:a', codec, '-compression_level', comp])
            else:
                args.extend(['-codec:a', codec])
        
        elif self.output_format == 'wav':
            args.extend(['-codec:a', codec])
        
        elif self.output_format == 'aiff':
            args.extend(['-codec:a', codec])
        
        elif self.output_format == 'alac':
            args.extend(['-codec:a', codec])
        
        elif self.output_format == 'wv':
            args.extend(['-codec:a', codec])
        
        elif self.output_format == 'ape':
            args.extend(['-codec:a', codec])
        
        elif self.output_format == 'tta':
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
        
        elif self.output_format == 'au':
            args.extend(['-codec:a', codec])
        
        elif self.output_format == 'mka':
            args.extend(['-codec:a', codec, '-q:a', '6'])
        
        elif self.output_format == 'oga':
            args.extend(['-codec:a', codec])
        
        elif self.output_format == 'caf':
            args.extend(['-codec:a', codec])
        
        elif self.output_format == 'shn':
            args.extend(['-codec:a', codec])
        
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
                    except:
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

        self._load_config()
        self._create_vars()
        self._build_ui()
        self._setup_hotkeys()
        self._setup_drop_targets()
        self._completed_count = 0
        self.ffmpeg_ok = self._check_ffmpeg()
        self._log(f"FFmpeg: {'найден' if self.ffmpeg_ok else 'НЕ НАЙДЕН'}", 'info' if self.ffmpeg_ok else 'error')

    def _create_vars(self):
        self.v_pitch       = tk.BooleanVar()
        self.v_pitch_val   = tk.DoubleVar(value=0.5)
        self.v_speed       = tk.BooleanVar()
        self.v_speed_val   = tk.DoubleVar(value=1.00)
        self.v_eq          = tk.BooleanVar()
        self.v_eq_type     = tk.IntVar(value=0)
        self.v_eq_val      = tk.DoubleVar(value=-2.0)
        self.v_silence     = tk.BooleanVar()
        self.v_silence_val = tk.IntVar(value=45)
        self.v_phase_inv     = tk.BooleanVar(value=True)
        self.v_phase_inv_val = tk.DoubleVar(value=1.0)
        self.v_phase_scr     = tk.BooleanVar(value=True)
        self.v_phase_scr_val = tk.DoubleVar(value=2.0)
        self.v_dc            = tk.BooleanVar(value=True)
        self.v_dc_val        = tk.DoubleVar(value=0.000005)
        self.v_resamp        = tk.BooleanVar(value=True)
        self.v_resamp_val    = tk.IntVar(value=1)
        self.v_ultra         = tk.BooleanVar(value=True)
        self.v_ultra_freq    = tk.IntVar(value=21000)
        self.v_ultra_level   = tk.DoubleVar(value=0.001)
        self.v_haas          = tk.BooleanVar(value=True)
        self.v_haas_val      = tk.DoubleVar(value=15.0)
        self.v_dither        = tk.BooleanVar(value=True)
        self.v_dither_method = tk.StringVar(value='triangular_hp')
        self.v_id3pad        = tk.BooleanVar(value=True)
        self.v_id3pad_val    = tk.IntVar(value=512)
        
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

        self.v_trim      = tk.BooleanVar()
        self.v_trim_val  = tk.DoubleVar(value=5.0)
        self.v_cut       = tk.BooleanVar()
        self.v_cut_pos   = tk.IntVar(value=50)
        self.v_cut_dur   = tk.DoubleVar(value=2.0)
        self.v_fade      = tk.BooleanVar()
        self.v_fade_val  = tk.DoubleVar(value=5.0)
        self.v_merge     = tk.BooleanVar()
        self.v_extra     = tk.StringVar()
        self.v_broken    = tk.BooleanVar()
        self.v_broken_t  = tk.IntVar(value=0)
        self.v_bitrate_j = tk.BooleanVar()
        self.v_frame_sh  = tk.BooleanVar()
        self.v_fake_meta = tk.BooleanVar()
        self.v_reorder   = tk.BooleanVar(value=True)
        self.v_preserve_meta   = tk.BooleanVar()
        self.v_preserve_cover  = tk.BooleanVar()
        self.v_rename          = tk.BooleanVar(value=True)
        self.v_delete_orig     = tk.BooleanVar()
        self.v_reupload        = tk.BooleanVar()
        self.v_quality         = tk.StringVar(value='245 kbps (VBR Q0)')
        self.v_title  = tk.StringVar()
        self.v_artist = tk.StringVar()
        self.v_album  = tk.StringVar()
        self.v_year   = tk.StringVar()
        self.v_genre  = tk.StringVar()
        self.v_reupload_text = tk.StringVar(value='(REUPLOAD)')
        self.v_reupload_pos  = tk.StringVar(value='after')
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
        ttk.Button(mode_frame, text="Модификатор", 
                   command=lambda: self._switch_mode('modifier')).pack(side='left', padx=2)
        ttk.Button(mode_frame, text="Конвертер",
                   command=lambda: self._switch_mode('converter')).pack(side='left', padx=2)
        
        self.lbl_mode = ttk.Label(top, text="Режим: Модификатор", font=('', 9, 'bold'), foreground='#6366f1')
        self.lbl_mode.pack(side='left', padx=10)
        
        self.lbl_ffmpeg = ttk.Label(top, text="FFmpeg: проверка...")
        self.lbl_ffmpeg.pack(side='right', padx=8)
        ttk.Separator(self.root, orient='horizontal').pack(fill='x')

        pw = ttk.PanedWindow(self.root, orient='horizontal')
        pw.pack(fill='both', expand=True, padx=4, pady=4)

        left = ttk.Frame(pw, width=280)
        left.pack_propagate(False)
        pw.add(left, weight=0)
        self._build_left(left)

        right_outer = ttk.Frame(pw)
        pw.add(right_outer, weight=1)
        self._build_right(right_outer)

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
        self.file_listbox = tk.Listbox(list_frame, yscrollcommand=sb.set, activestyle='dotbox',
                                       selectbackground='#6366f1', selectforeground='white',
                                       exportselection=False)
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
        canvas = tk.Canvas(self.modifier_frame, borderwidth=0, highlightthickness=0)
        vsb = ttk.Scrollbar(self.modifier_frame, orient='vertical', command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side='right', fill='y')
        canvas.pack(side='left', fill='both', expand=True)

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
        canvas.bind_all('<MouseWheel>', _on_wheel)

        f = self._scroll_frame
        p = dict(padx=6, pady=4)

        row1 = ttk.Frame(f)
        row1.pack(fill='x', **p)
        self._build_cover_section(row1)
        self._build_metadata_section(row1)
        self._build_track_info_section(row1)

        self._build_waveform_section(f)
        self._build_preset_buttons(f)
        self._build_methods_notebook(f)

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
        ttk.Label(header, text="Поддерживается 26 форматов", 
                  foreground='#888', font=('', 9)).pack(side='right')
        
        ttk.Separator(f, orient='horizontal').pack(fill='x', padx=6)
        
        settings_frame = ttk.LabelFrame(f, text="Настройки конвертации", padding=8)
        settings_frame.pack(fill='x', padx=6, pady=4)
        
        fmt_row = ttk.Frame(settings_frame)
        fmt_row.pack(fill='x', pady=4)
        ttk.Label(fmt_row, text="Выходной формат:", font=('', 9, 'bold')).pack(side='left', padx=4)
        
        self.cmb_conv_format = ttk.Combobox(fmt_row, textvariable=self.v_conv_format,
                                            values=list(SUPPORTED_FORMATS.keys()),
                                            width=10, state='readonly')
        self.cmb_conv_format.pack(side='left', padx=4)
        self.cmb_conv_format.bind('<<ComboboxSelected>>', self._on_format_changed)
        
        self.lbl_format_desc = ttk.Label(fmt_row, text="", foreground='#888', font=('', 8))
        self.lbl_format_desc.pack(side='left', padx=10)
        
        self.quality_frame = ttk.Frame(settings_frame)
        self.quality_frame.pack(fill='x', pady=4)
        ttk.Label(self.quality_frame, text="Качество:", font=('', 9, 'bold')).pack(side='left', padx=4)
        
        self.cmb_conv_quality = ttk.Combobox(self.quality_frame, textvariable=self.v_conv_quality,
                                             width=25, state='readonly')
        self.cmb_conv_quality.pack(side='left', padx=4)
        
        out_frame = ttk.LabelFrame(f, text="Настройки вывода", padding=8)
        out_frame.pack(fill='x', padx=6, pady=4)
        
        dir_row = ttk.Frame(out_frame)
        dir_row.pack(fill='x', pady=2)
        ttk.Button(dir_row, text="Выбрать папку", command=self._select_output_dir).pack(side='left')
        self.lbl_out_dir_conv = ttk.Label(dir_row, text=self.output_dir, relief='sunken',
                                     padding=2, width=30)
        self.lbl_out_dir_conv.pack(side='left', padx=4, fill='x', expand=True)
        
        ttk.Checkbutton(out_frame, text="Удалять оригиналы после конвертации",
                       variable=self.v_conv_delete).pack(anchor='w', pady=2)
        
        info_frame = ttk.LabelFrame(f, text="Информация о поддерживаемых форматах", padding=8)
        info_frame.pack(fill='both', expand=True, padx=6, pady=4)
        
        self.format_info_text = tk.Text(info_frame, height=10, font=('Courier', 9), 
                                        wrap='word', state='disabled')
        scroll = ttk.Scrollbar(info_frame, orient='vertical', command=self.format_info_text.yview)
        self.format_info_text.configure(yscrollcommand=scroll.set)
        scroll.pack(side='right', fill='y')
        self.format_info_text.pack(fill='both', expand=True)
        
        self._update_format_info()
        self._on_format_changed()
        
        action_frame = ttk.Frame(f)
        action_frame.pack(fill='x', padx=6, pady=4)
        
        self.conv_progress_var = tk.IntVar()
        self.conv_progress_bar = ttk.Progressbar(action_frame, variable=self.conv_progress_var, maximum=100)
        self.conv_progress_bar.pack(side='left', fill='x', expand=True, padx=(0, 8))
        
        self.btn_convert = ttk.Button(action_frame, text="Запустить конвертацию", 
                                      command=self._start_conversion)
        self.btn_convert.pack(side='right')
        
        log_frame = ttk.LabelFrame(f, text="Лог конвертации", padding=4)
        log_frame.pack(fill='both', expand=True, padx=6, pady=(0, 6))
        
        self.conv_log_text = scrolledtext.ScrolledText(log_frame, height=8, state='disabled',
                                                       font=('Courier', 9), wrap='word')
        self.conv_log_text.pack(fill='both', expand=True)
        self.conv_log_text.tag_config('info', foreground='#333333')
        self.conv_log_text.tag_config('success', foreground='#007700')
        self.conv_log_text.tag_config('warning', foreground='#aa6600')
        self.conv_log_text.tag_config('error', foreground='#cc0000')
    
    def _update_format_info(self):
        self.format_info_text.config(state='normal')
        self.format_info_text.delete('1.0', 'end')
        
        info = """Поддерживаемые форматы (26 форматов):

Lossy форматы (сжатие с потерями):
  - MP3: самый популярный формат, универсальный
  - AAC/M4A: современный формат, лучше качество при меньшем размере
  - OGG Vorbis: открытый формат, хорошее качество
  - Opus: самый эффективный lossy кодек
  - WMA: формат Microsoft
  - AC3/Dolby Digital: многоканальный звук для DVD
  - DTS: формат для кинотеатров
  - MP2/MPEG Layer 2: предшественник MP3
  - Musepack (MPC): высококачественный lossy
  - Speex: оптимизирован для речи
  - AMR: мобильный речевой кодек

Lossless форматы (без потерь):
  - FLAC: самый популярный lossless формат
  - WAV: несжатый PCM аудио
  - AIFF: несжатый формат Apple
  - ALAC: Apple Lossless
  - WavPack: гибридный lossless/lossy
  - Monkey's Audio (APE): высокая компрессия
  - True Audio (TTA): быстрый lossless
  - Shorten (SHN): исторический lossless
  - Ogg FLAC (OGA): FLAC в контейнере Ogg

Контейнеры и другие:
  - Matroska Audio (MKA): аудиоконтейнер MKV
  - Core Audio Format (CAF): контейнер Apple
  - AU/Sun Audio: формат Unix-систем

Все форматы конвертируются между собой в любых направлениях."""
        
        self.format_info_text.insert('1.0', info)
        self.format_info_text.config(state='disabled')
    
    def _on_format_changed(self, event=None):
        fmt = self.v_conv_format.get()
        
        desc = SUPPORTED_FORMATS.get(fmt, '')
        self.lbl_format_desc.config(text=desc)
        
        if fmt in QUALITY_PRESETS:
            self.cmb_conv_quality['values'] = QUALITY_PRESETS[fmt]
            self.cmb_conv_quality.current(0)
            self.cmb_conv_quality.config(state='readonly')
        elif fmt in ['wav', 'aiff', 'au', 'caf']:
            self.cmb_conv_quality['values'] = ['Uncompressed PCM']
            self.cmb_conv_quality.current(0)
            self.cmb_conv_quality.config(state='disabled')
        elif fmt == 'flac':
            self.cmb_conv_quality['values'] = [
                'Compression 0 (fast)',
                'Compression 5 (default)',
                'Compression 8 (best)',
                'Compression 12 (max)'
            ]
            self.cmb_conv_quality.current(2)
            self.cmb_conv_quality.config(state='readonly')
        elif fmt == 'alac':
            self.cmb_conv_quality['values'] = ['Lossless']
            self.cmb_conv_quality.current(0)
            self.cmb_conv_quality.config(state='disabled')
        elif fmt in ['wv', 'ape', 'tta', 'shn']:
            self.cmb_conv_quality['values'] = ['Lossless / Default']
            self.cmb_conv_quality.current(0)
            self.cmb_conv_quality.config(state='disabled')
        elif fmt in ['ac3', 'dts', 'mp2', 'mpc', 'spx', 'amr', 'mka', 'oga']:
            self.cmb_conv_quality['values'] = ['Default quality']
            self.cmb_conv_quality.current(0)
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
        
        self._log(f"Запущена конвертация {len(self.input_files)} файлов в {output_format.upper()}...", 
                  'info', to_converter=True)
        
        converter = BatchConverter(
            files=list(self.input_files),
            output_dir=self.output_dir,
            output_format=output_format,
            quality_preset=quality,
            result_queue=self._worker_queue,
            max_workers=max_workers,
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

        self.lbl_cover = ttk.Label(lf, text="(нет)", width=20, anchor='center',
                                   relief='groove', padding=4)
        self.lbl_cover.pack()

        ttk.Button(lf, text="Загрузить", command=self._select_cover).pack(fill='x', pady=1)
        ttk.Button(lf, text="Рандом",    command=self._random_cover).pack(fill='x', pady=1)
        self.btn_rm_cover = ttk.Button(lf, text="Удалить", command=self._remove_cover, state='disabled')
        self.btn_rm_cover.pack(fill='x', pady=1)

    def _build_metadata_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Метаданные", padding=6)
        lf.pack(side='left', fill='both', expand=True, padx=(0, 4))

        fields = [("Название",   self.v_title),
                  ("Исполнитель",self.v_artist),
                  ("Альбом",     self.v_album),
                  ("Год",        self.v_year),
                  ("Жанр",       self.v_genre)]
        for row_i, (lbl, var) in enumerate(fields):
            ttk.Label(lf, text=lbl).grid(row=row_i, column=0, sticky='w', padx=2, pady=1)
            e = ttk.Entry(lf, textvariable=var)
            e.grid(row=row_i, column=1, sticky='ew', padx=2, pady=1)
            var.trace_add('write', lambda *_: self._update_name_preview())
        lf.columnconfigure(1, weight=1)

        btn_row = ttk.Frame(lf)
        btn_row.grid(row=len(fields), column=0, columnspan=2, pady=4)
        ttk.Button(btn_row, text="Копировать из оригинала", command=self._copy_meta).pack(side='left', padx=2)
        ttk.Button(btn_row, text="Рандом",                  command=self._random_meta).pack(side='left', padx=2)
        ttk.Button(btn_row, text="Очистить",                command=self._clear_meta).pack(side='left', padx=2)

        r = len(fields) + 1
        dec_row = ttk.Frame(lf)
        dec_row.grid(row=r, column=0, columnspan=2, sticky='ew', pady=(4, 0))
        ttk.Checkbutton(dec_row, text="Добавить текст:", variable=self.v_reupload,
                        command=self._update_name_preview).pack(side='left')
        ttk.Entry(dec_row, textvariable=self.v_reupload_text, width=14).pack(side='left', padx=2)
        ttk.Radiobutton(dec_row, text="До", variable=self.v_reupload_pos,
                        value='before', command=self._update_name_preview).pack(side='left')
        ttk.Radiobutton(dec_row, text="После", variable=self.v_reupload_pos,
                        value='after', command=self._update_name_preview).pack(side='left')
        self.v_reupload_text.trace_add('write', lambda *_: self._update_name_preview())

        r += 1
        self.lbl_title_preview = ttk.Label(lf, text="Предпросмотр: —",
                                           foreground='#888', font=('', 8))
        self.lbl_title_preview.grid(row=r, column=0, columnspan=2, sticky='w', pady=(2, 0))

    def _update_name_preview(self):
        import re as _re

        title  = self.v_title.get()
        artist = self.v_artist.get()
        album  = self.v_album.get()
        year   = self.v_year.get()

        display = title or (artist and f"{artist} - {title}") or "Название"
        if artist and title:
            display = f"{artist} - {title}"
        elif title:
            display = title
        else:
            display = "(нет названия)"

        if self.v_reupload.get():
            text = self.v_reupload_text.get()
            if self.v_reupload_pos.get() == 'before':
                display = f"{text} {display}"
            else:
                display = f"{display} {text}"

        try:
            self.lbl_title_preview.config(text=f"Предпросмотр: {display}")
        except AttributeError:
            pass

        def _safe(s):
            return _re.sub(r'[\\/*?:"<>|]', '_', s).strip() or '_'

        tpl = self.v_filename_template.get() or 'VK_{n:03d}_custom'

        if self.current_index >= 0 and self.current_index < len(self.tracks_info):
            ti = self.tracks_info[self.current_index]
            orig = os.path.splitext(os.path.basename(self.input_files[self.current_index]))[0]
            ex_title  = title or ti.title or orig
            ex_artist = artist or ti.artist or ''
            ex_album  = album or ti.album or ''
            ex_year   = year or ti.year or ''
        else:
            orig = 'example_track'
            ex_title  = title or 'Название'
            ex_artist = artist or 'Исполнитель'
            ex_album  = album or ''
            ex_year   = year or ''

        try:
            fname = tpl.format(
                n=1, original=_safe(orig),
                title=_safe(ex_title), artist=_safe(ex_artist),
                album=_safe(ex_album), year=_safe(ex_year),
            ) + '.mp3'
        except (KeyError, ValueError) as e:
            fname = f"ERROR: {e}"

        try:
            self.lbl_file_preview.config(text=f"Предпросмотр: {fname}")
        except AttributeError:
            pass

    def _build_track_info_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Анализ трека", padding=6)
        lf.pack(side='left', fill='y')

        self.txt_track_info = tk.Text(lf, width=30, height=8, state='disabled',
                                      font=('Courier', 9), wrap='none')
        self.txt_track_info.pack(fill='both', expand=True)
        self._update_track_info(-1)

    def _build_preset_buttons(self, parent):
        lf = ttk.LabelFrame(parent, text="Быстрые пресеты", padding=4)
        lf.pack(fill='x', padx=6, pady=4)
        self._quick_presets_frame = ttk.Frame(lf)
        self._quick_presets_frame.pack(fill='x')
        self._refresh_quick_presets()

    def _refresh_quick_presets(self):
        for w in self._quick_presets_frame.winfo_children():
            w.destroy()
        if not self.saved_presets:
            ttk.Label(self._quick_presets_frame, text="Нет сохранённых пресетов",
                      foreground='gray', font=('TkDefaultFont', 8, 'italic')).pack(side='left', padx=4)
            return
        for i, p in enumerate(self.saved_presets):
            name = p.get('name', f'Preset {i+1}')
            ttk.Button(self._quick_presets_frame, text=name,
                       command=lambda idx=i: self._apply_user_preset(idx)).pack(side='left', padx=4)

    def _apply_user_preset(self, index):
        self.preset_listbox.selection_clear(0, 'end')
        self.preset_listbox.selection_set(index)
        self._load_selected_preset()

    def _build_methods_notebook(self, parent):
        nb = ttk.Notebook(parent)
        nb.pack(fill='x', padx=6, pady=4)

        self._build_basic_tab(nb)
        self._build_spectral_tab(nb)
        self._build_texture_tab(nb)
        self._build_advanced_tab(nb)
        self._build_technical_tab(nb)
        self._build_system_tab(nb)

        for v in (
            self.v_fade, self.v_fade_val,
            self.v_trim, self.v_trim_val,
            self.v_speed, self.v_speed_val,
            self.v_pitch, self.v_pitch_val,
            self.v_eq, self.v_eq_type, self.v_eq_val,
            self.v_silence, self.v_silence_val,
            self.v_phase_inv, self.v_phase_inv_val,
            self.v_phase_scr, self.v_phase_scr_val,
            self.v_dc, self.v_dc_val,
            self.v_resamp, self.v_resamp_val,
            self.v_ultra, self.v_ultra_level,
            self.v_haas, self.v_haas_val,
            self.v_cut, self.v_cut_pos, self.v_cut_dur,
            self.v_spectral_mask, self.v_spectral_mask_att, self.v_spectral_mask_peaks,
            self.v_concert_emu, self.v_concert_intensity,
            self.v_midside, self.v_midside_mid, self.v_midside_side,
            self.v_psycho_noise, self.v_psycho_intensity,
            self.v_temp_jitter, self.v_jitter_intensity, self.v_jitter_freq,
            self.v_spec_jitter, self.v_spec_jitter_count, self.v_spec_jitter_att,
            self.v_saturation, self.v_saturation_drive, self.v_saturation_mix,
        ):
            v.trace_add('write', lambda *a: self._schedule_preview_update())

    def _method_row(self, parent, row, text, var, *extra_widgets):
        cb = ttk.Checkbutton(parent, text=text, variable=var, command=self._check_conflicts)
        cb.grid(row=row, column=0, sticky='w', padx=4, pady=2)
        for col, w in enumerate(extra_widgets, start=1):
            w.grid(row=row, column=col, padx=4, pady=2, sticky='w')
        return cb

    def _spin(self, parent, var, from_, to, inc, width=8, fmt='%.2f'):
        sb = ttk.Spinbox(parent, textvariable=var, from_=from_, to=to,
                         increment=inc, width=width, format=fmt if isinstance(var, tk.DoubleVar) else None)
        return sb

    def _desc(self, parent, row, col, text, colspan=4):
        ttk.Label(parent, text=text, foreground='gray', font=('', 8, 'italic')
                  ).grid(row=row, column=col, columnspan=colspan, sticky='w', padx=22, pady=(0, 3))

    def _build_waveform_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Предпросмотр формы сигнала", padding=4)
        lf.pack(fill='x', padx=6, pady=4)

        hdr = ttk.Frame(lf)
        hdr.pack(fill='x', pady=(0, 2))
        ttk.Label(hdr, text="ДО изменений", font=('', 8, 'bold'),
                  foreground='#5599ff').pack(side='left', padx=6)
        self.lbl_wave_status = ttk.Label(hdr, text="Выберите файл",
                                          foreground='gray', font=('', 8))
        self.lbl_wave_status.pack(side='left', padx=20)
        ttk.Label(hdr, text="ПОСЛЕ изменений", font=('', 8, 'bold'),
                  foreground='#44dd44').pack(side='right', padx=6)

        wave_row = ttk.Frame(lf)
        wave_row.pack(fill='x')
        wave_row.columnconfigure(0, weight=1)
        wave_row.columnconfigure(1, weight=1)

        self.canvas_before = tk.Canvas(wave_row, height=160, bg='#0d1117',
                                        highlightthickness=1,
                                        highlightbackground='#30304a')
        self.canvas_before.grid(row=0, column=0, sticky='ew', padx=(2, 1), pady=2)

        self.canvas_after = tk.Canvas(wave_row, height=160, bg='#0d170d',
                                       highlightthickness=1,
                                       highlightbackground='#2a3a2a')
        self.canvas_after.grid(row=0, column=1, sticky='ew', padx=(1, 2), pady=2)

        self.canvas_before.bind('<Configure>', lambda e: self._schedule_redraw())
        self.canvas_after.bind('<Configure>', lambda e: self._schedule_redraw())

    def _load_waveform_for_file(self, file_path):
        if self._waveform_loading:
            return
        self._waveform_loading = True
        self._waveform_samples = None
        self._draw_placeholder(self.canvas_before, 'Загрузка...')
        self._draw_placeholder(self.canvas_after, '')

        def _load():
            try:
                cmd = ['ffmpeg', '-i', file_path,
                       '-f', 's16le', '-ac', '1', '-ar', '500', '-']
                res = subprocess.run(cmd, capture_output=True, timeout=60)
                if res.returncode == 0 and res.stdout:
                    n = len(res.stdout) // 2
                    raw = struct.unpack(f'{n}h', res.stdout)
                    samples = [s / 32768.0 for s in raw]
                    self._waveform_samples = samples
                    self.root.after(0, self._on_waveform_loaded)
                else:
                    self.root.after(0, lambda: self._draw_placeholder(
                        self.canvas_before, 'Ошибка декодирования'))
            except Exception as e:
                self.root.after(0, lambda: self._draw_placeholder(
                    self.canvas_before, f'Ошибка: {e}'))
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
                'vk_infra':      self.v_vk_infra.get(),
                'vk_infra_amp':  self.v_vk_infra_amplitude.get(),
                'vk_infra_freq': self.v_vk_infra_freq.get(),
                'vk_infra_mode': self.v_vk_infra_mode.get(),
                'vk_infra_mod_freq': self.v_vk_infra_mod_freq.get(),
                'vk_infra_mod_depth': self.v_vk_infra_mod_depth.get(),
                'vk_infra_phase': self.v_vk_infra_phase_shift.get(),
                'vk_infra_waveform': self.v_vk_infra_waveform.get(),
                'vk_infra_harmonics': [self.v_vk_infra_h1.get(),
                                       self.v_vk_infra_h2.get(),
                                       self.v_vk_infra_h3.get()],
                'fade':          self.v_fade.get(),
                'fade_val':      self.v_fade_val.get(),
                'trim':          self.v_trim.get(),
                'trim_val':      self.v_trim_val.get(),
                'speed':         self.v_speed.get(),
                'speed_val':     self.v_speed_val.get(),
                'pitch':         self.v_pitch.get(),
                'pitch_val':     self.v_pitch_val.get(),
                'eq':            self.v_eq.get(),
                'eq_type':       self.v_eq_type.get(),
                'eq_val':        self.v_eq_val.get(),
                'silence':       self.v_silence.get(),
                'silence_val':   self.v_silence_val.get(),
                'phase_inv':     self.v_phase_inv.get(),
                'phase_inv_val': self.v_phase_inv_val.get(),
                'phase_scr':     self.v_phase_scr.get(),
                'phase_scr_val': self.v_phase_scr_val.get(),
                'dc':            self.v_dc.get(),
                'dc_val':        self.v_dc_val.get(),
                'resamp':        self.v_resamp.get(),
                'resamp_val':    self.v_resamp_val.get(),
                'ultra':         self.v_ultra.get(),
                'ultra_level':   self.v_ultra_level.get(),
                'haas':          self.v_haas.get(),
                'haas_val':      self.v_haas_val.get(),
                'saturation':    self.v_saturation.get(),
                'sat_drive':     self.v_saturation_drive.get(),
                'sat_mix':       self.v_saturation_mix.get(),
                'cut':           self.v_cut.get(),
                'cut_pos':       self.v_cut_pos.get(),
                'cut_dur':       self.v_cut_dur.get(),
                'spectral_mask':       self.v_spectral_mask.get(),
                'spectral_mask_att':   self.v_spectral_mask_att.get(),
                'spectral_mask_peaks': self.v_spectral_mask_peaks.get(),
                'concert':             self.v_concert_emu.get(),
                'concert_intensity':   self.v_concert_intensity.get(),
                'midside':             self.v_midside.get(),
                'midside_mid':         self.v_midside_mid.get(),
                'midside_side':        self.v_midside_side.get(),
                'psycho':              self.v_psycho_noise.get(),
                'psycho_intensity':    self.v_psycho_intensity.get(),
                'temp_jitter':         self.v_temp_jitter.get(),
                'jitter_intensity':    self.v_jitter_intensity.get(),
                'jitter_freq':         self.v_jitter_freq.get(),
                'spec_jitter':         self.v_spec_jitter.get(),
                'spec_jitter_count':   self.v_spec_jitter_count.get(),
                'spec_jitter_att':     self.v_spec_jitter_att.get(),
            }
        except tk.TclError:
            return

        samples = self._waveform_samples

        def _compute():
            preview = _compute_preview_static(samples, snap)
            self.root.after(0, lambda: self._draw_waveform(
                self.canvas_after, preview, '#44dd44'))

        threading.Thread(target=_compute, daemon=True).start()

    def _draw_placeholder(self, canvas, text):
        canvas.delete('all')
        w = canvas.winfo_width() or 200
        h = canvas.winfo_height() or 80
        canvas.create_line(0, h // 2, w, h // 2, fill='#333', width=1)
        if text:
            canvas.create_text(w // 2, h // 2, text=text,
                               fill='gray', font=('', 8))

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

        canvas.create_line(0, mid, w, mid, fill='#2a2a2a', width=1)

        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)
        inner_color = f'#{int(r*0.55):02x}{int(g*0.55):02x}{int(b*0.55):02x}'

        for x in range(w):
            i0 = int(x * n / w)
            i1 = int((x + 1) * n / w)
            if i1 <= i0:
                i1 = i0 + 1
            chunk = samples[i0:min(i1, n)]
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

    def _clear_waveforms(self):
        self._waveform_samples = None
        self._draw_placeholder(self.canvas_before, 'Выберите файл')
        self._draw_placeholder(self.canvas_after, '')
        self.lbl_wave_status.config(text='Выберите файл')

    def _build_basic_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Базовые")
        f.columnconfigure(0, weight=0)

        r = 0
        ttk.Checkbutton(f, text="Изменить тональность (Pitch Shift)", variable=self.v_pitch,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_pitch_val, -5.0, 5.0, 0.5).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="семитонов").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Транспонирует аудио на ±N полутонов без изменения темпа. "
                             "При значениях до ±2 изменение практически неслышимо.")

        r += 1
        ttk.Checkbutton(f, text="Изменить скорость (Time Stretch)", variable=self.v_speed,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_speed_val, 0.90, 1.10, 0.01).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="x").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Ускоряет или замедляет трек с сохранением тональности. "
                             "Значения 0.97–1.03 не воспринимаются на слух.")

        r += 1
        ttk.Checkbutton(f, text="Эквализация (EQ)", variable=self.v_eq,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        eq_types = ["Стандарт: -2dB на 1 kHz", "Пресет Mid-Cut: 1k/2k -4/-2dB", "Пресет Air: 8k +3dB"]
        self.cmb_eq_type = ttk.Combobox(f, values=eq_types, width=26, state='readonly')
        self.cmb_eq_type.current(0)
        self.cmb_eq_type.grid(row=r, column=1, columnspan=2, padx=4, pady=(4, 0))
        self._spin(f, self.v_eq_val, -12.0, 12.0, 1.0, width=5).grid(row=r, column=3, padx=2, pady=(4, 0))
        ttk.Label(f, text="dB").grid(row=r, column=4, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Ослабляет или усиливает выбранную частотную полосу. "
                             "Mid-Cut убирает середину, Air добавляет яркость на верхах.")

        r += 1
        ttk.Checkbutton(f, text="Добавить тишину в конец (Silent Pad)", variable=self.v_silence,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_silence_val, 1, 300, 1, width=6, fmt=None).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="сек").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Добавляет тишину в конец файла, увеличивая его длительность и меняя хэш. "
                             "Не совмещать с Fade Out.")

        for v in (self.v_pitch_val, self.v_speed_val, self.v_eq_val, self.v_silence_val):
            v.trace_add('write', lambda *a: self._check_conflicts())

    def _build_spectral_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Спектральные")

        rows_data = [
            ("Phase Invert - инверсия фазы правого канала",
             self.v_phase_inv,
             self._spin(f, self.v_phase_inv_val, 0.0, 1.0, 0.1), "сила",
             "Инвертирует фазу правого канала, меняя пространственный отпечаток файла. "
             "Сила 1.0 - полная инверсия, 0.5 - частичная."),
            ("Phase Scramble - синусоидальная модуляция фазы (фазер)",
             self.v_phase_scr,
             self._spin(f, self.v_phase_scr_val, 0.1, 5.0, 0.1), "Гц",
             "Синусоидально модулирует фазу сигнала, нарушая спектральный отпечаток. "
             "При частоте <=2 Гц изменение не воспринимается на слух."),
            ("DC Shift - постоянное смещение нуля",
             self.v_dc,
             self._spin(f, self.v_dc_val, 0.0, 0.0001, 0.000001, fmt='%.6f'), "",
             "Добавляет постоянное смещение к каждому сэмплу, незаметно меняя хэш и спектрограмму. "
             "Значение 0.000005 неотличимо на слух."),
            ("Resample Drift - дрейф частоты дискретизации",
             self.v_resamp,
             self._spin(f, self.v_resamp_val, -100, 100, 1, fmt=None), "Гц",
             "Пересэмплирует аудио с отклонением ±N Гц и обратно, оставляя уникальный спектральный артефакт. "
             "Рекомендуется ±1-3 Гц."),
            ("Haas Delay - стереоэффект задержки Хааса",
             self.v_haas,
             self._spin(f, self.v_haas_val, 0.0, 50.0, 0.5), "мс",
             "Задерживает правый канал на N мс. При 5-30 мс эффект не слышен, "
             "но полностью меняет стереокорреляцию файла."),
        ]

        r = 0
        for title, var, spin, unit, desc in rows_data:
            ttk.Checkbutton(f, text=title, variable=var,
                            command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
            spin.grid(row=r, column=1, padx=4, pady=(4, 0))
            if unit:
                ttk.Label(f, text=unit).grid(row=r, column=2, sticky='w', pady=(4, 0))
            r += 1
            self._desc(f, r, 0, desc)
            r += 1

        ttk.Checkbutton(f, text="Ultrasonic Noise - подмешивание ультразвука",
                        variable=self.v_ultra,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        sub_u = ttk.Frame(f)
        sub_u.grid(row=r, column=1, columnspan=3, sticky='w', pady=(4, 0))
        ttk.Label(sub_u, text="Freq:").pack(side='left')
        self._spin(sub_u, self.v_ultra_freq, 20000, 48000, 100, width=7, fmt=None).pack(side='left', padx=2)
        ttk.Label(sub_u, text="Hz  Level:").pack(side='left')
        self._spin(sub_u, self.v_ultra_level, 0.0, 0.01, 0.0001, fmt='%.4f').pack(side='left', padx=2)
        r += 1
        self._desc(f, r, 0, "Подмешивает синусоиду выше 20 кГц - неслышимую, но полностью "
                             "меняющую ультразвуковую часть спектра. Level <= 0.003.")
        r += 1

        ttk.Checkbutton(f, text="Dither Attack - целенаправленный шум квантования",
                        variable=self.v_dither,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        ttk.Combobox(f, textvariable=self.v_dither_method, width=16, state='readonly',
                     values=['triangular_hp', 'rectangular', 'gaussian', 'lipshitz']
                     ).grid(row=r, column=1, padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "При конвертации в MP3 применяет выбранный алгоритм дитеринга, "
                             "формируя уникальный шумовой профиль в тихих участках.")
        r += 1

        ttk.Checkbutton(f, text="ID3 Padding Attack - мусорные данные в тегах",
                        variable=self.v_id3pad,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_id3pad_val, 0, 2048, 64, width=6, fmt=None).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="байт").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Вставляет случайный мусор в теги ID3v2. "
                             "Меняет MD5, размер и структуру файла, не затрагивая аудио.")

        for v in (self.v_phase_scr_val, self.v_resamp_val, self.v_ultra_level):
            v.trace_add('write', lambda *a: self._check_conflicts())

    def _build_texture_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Текстурные")
        f.columnconfigure(0, weight=0)

        r = 0
        ttk.Checkbutton(f, text="Спектральное маскирование (Spectral Masking)",
                        variable=self.v_spectral_mask,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        
        mask_frame = ttk.Frame(f)
        mask_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=22, pady=2)
        
        ttk.Label(mask_frame, text="Чувствительность:").grid(row=0, column=0, sticky='w', padx=2)
        self._spin(mask_frame, self.v_spectral_mask_sens, 0.3, 1.5, 0.1, width=6).grid(row=0, column=1, padx=2)
        ttk.Label(mask_frame, text="(0.5-1.2)").grid(row=0, column=2, sticky='w', padx=2)
        
        ttk.Label(mask_frame, text="Подавление:").grid(row=0, column=3, sticky='w', padx=(20,2))
        self._spin(mask_frame, self.v_spectral_mask_att, 5, 20, 1, width=5, fmt=None).grid(row=0, column=4, padx=2)
        ttk.Label(mask_frame, text="dB").grid(row=0, column=5, sticky='w', padx=2)
        
        ttk.Label(mask_frame, text="Макс. пиков:").grid(row=0, column=6, sticky='w', padx=(20,2))
        self._spin(mask_frame, self.v_spectral_mask_peaks, 5, 20, 1, width=5, fmt=None).grid(row=0, column=7, padx=2)
        
        r += 1
        self._desc(f, r, 0, "Анализирует спектр и точечно подавляет наиболее энергичные частоты. "
                             "Разрушает акустические якоря отпечатка без заметных изменений на слух.")
        r += 1
        ttk.Checkbutton(f, text="Эмуляция концертной записи (Concert Emulation)",
                        variable=self.v_concert_emu,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        
        concert_frame = ttk.Frame(f)
        concert_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=22, pady=2)
        
        ttk.Label(concert_frame, text="Интенсивность:").pack(side='left', padx=2)
        cmb_concert = ttk.Combobox(concert_frame, textvariable=self.v_concert_intensity,
                                   values=['light', 'medium', 'heavy'], width=10, state='readonly')
        cmb_concert.current(1)
        cmb_concert.pack(side='left', padx=5)
        ttk.Label(concert_frame, text="(light/medium/heavy)").pack(side='left', padx=2)
        
        r += 1
        self._desc(f, r, 0, "Имитирует запись в зале или радиоэфир: реверберация, "
                             "сужение стереобазы, компрессия и лёгкая эквализация.")
        r += 1
        ttk.Checkbutton(f, text="Mid/Side обработка",
                        variable=self.v_midside,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        
        ms_frame = ttk.Frame(f)
        ms_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=22, pady=2)
        
        ttk.Label(ms_frame, text="Mid (центр):").grid(row=0, column=0, sticky='w', padx=2)
        self._spin(ms_frame, self.v_midside_mid, -12.0, 6.0, 0.5, width=6).grid(row=0, column=1, padx=2)
        ttk.Label(ms_frame, text="dB").grid(row=0, column=2, sticky='w', padx=2)
        
        ttk.Label(ms_frame, text="Side (бока):").grid(row=0, column=3, sticky='w', padx=(20,2))
        self._spin(ms_frame, self.v_midside_side, -6.0, 12.0, 0.5, width=6).grid(row=0, column=4, padx=2)
        ttk.Label(ms_frame, text="dB").grid(row=0, column=5, sticky='w', padx=2)
        
        r += 1
        self._desc(f, r, 0, "Независимо регулирует уровень Mid (центр: вокал, бас) и Side (бока: атмосфера, реверб). "
                             "Меняет пространство звука без явных артефактов.")
        r += 1
        ttk.Checkbutton(f, text="Психоакустический шум (Psychoacoustic Noise)",
                        variable=self.v_psycho_noise,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        
        noise_frame = ttk.Frame(f)
        noise_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=22, pady=2)
        
        ttk.Label(noise_frame, text="Интенсивность:").pack(side='left', padx=2)
        self._spin(noise_frame, self.v_psycho_intensity, 0.0001, 0.002, 0.0001, fmt='%.4f', width=7).pack(side='left', padx=5)
        ttk.Label(noise_frame, text="(0.0002-0.0008)").pack(side='left', padx=2)
        
        r += 1
        self._desc(f, r, 0, "Вводит высокочастотный интерференционный шум, скрытый за основным сигналом. "
                             "На слух не различим, но меняет спектральный отпечаток.")
        r += 1
        ttk.Checkbutton(f, text="Аналоговое насыщение (Saturation)",
                        variable=self.v_saturation,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        
        sat_frame = ttk.Frame(f)
        sat_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=22, pady=2)
        
        ttk.Label(sat_frame, text="Drive:").grid(row=0, column=0, sticky='w', padx=2)
        self._spin(sat_frame, self.v_saturation_drive, 1.0, 3.0, 0.1, width=6).grid(row=0, column=1, padx=2)
        
        ttk.Label(sat_frame, text="Mix:").grid(row=0, column=2, sticky='w', padx=(20,2))
        self._spin(sat_frame, self.v_saturation_mix, 0.05, 0.4, 0.05, width=6).grid(row=0, column=3, padx=2)
        
        r += 1
        self._desc(f, r, 0, "Добавляет мягкие гармонические искажения в стиле аналоговой ленты. "
                             "Drive - насыщенность; Mix - доля эффекта в итоговом сигнале.")
        r += 1
        ttk.Checkbutton(f, text="Временной джиттер (Temporal Jitter)",
                        variable=self.v_temp_jitter,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        
        jitter_frame = ttk.Frame(f)
        jitter_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=22, pady=2)
        
        ttk.Label(jitter_frame, text="Амплитуда:").grid(row=0, column=0, sticky='w', padx=2)
        self._spin(jitter_frame, self.v_jitter_intensity, 0.001, 0.01, 0.001, fmt='%.3f', width=7).grid(row=0, column=1, padx=2)
        
        ttk.Label(jitter_frame, text="Частота:").grid(row=0, column=2, sticky='w', padx=(20,2))
        self._spin(jitter_frame, self.v_jitter_freq, 0.1, 2.0, 0.1, width=6).grid(row=0, column=3, padx=2)
        ttk.Label(jitter_frame, text="Гц").grid(row=0, column=4, sticky='w', padx=2)
        
        r += 1
        self._desc(f, r, 0, "Вводит синусоидальные микровариации скорости, имитируя нестабильность "
                             "аналоговых носителей. Не слышно при амплитуде < 0.003.")
        r += 1
        ttk.Checkbutton(f, text="Спектральный джиттер (Spectral Jitter)",
                        variable=self.v_spec_jitter,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1
        
        sj_frame = ttk.Frame(f)
        sj_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=22, pady=2)
        
        ttk.Label(sj_frame, text="Провалов:").grid(row=0, column=0, sticky='w', padx=2)
        self._spin(sj_frame, self.v_spec_jitter_count, 3, 12, 1, width=5, fmt=None).grid(row=0, column=1, padx=2)
        
        ttk.Label(sj_frame, text="Подавление:").grid(row=0, column=2, sticky='w', padx=(20,2))
        self._spin(sj_frame, self.v_spec_jitter_att, 8, 20, 1, width=5, fmt=None).grid(row=0, column=3, padx=2)
        ttk.Label(sj_frame, text="dB").grid(row=0, column=4, sticky='w', padx=2)
        
        r += 1
        self._desc(f, r, 0, "Случайно расставляет узкие провалы в спектре. В отличие от Spectral Masking, "
                             "позиции не зависят от анализа - каждый трек получает уникальный паттерн.")
        r += 1

        ttk.Separator(f, orient='horizontal').grid(row=r, column=0, columnspan=5, sticky='ew', pady=6)
        r += 1

        ttk.Checkbutton(f, text="VK Инфразвук 10 Гц (Anti-VK Content ID)",
                        variable=self.v_vk_infra,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        r += 1

        vk_frame = ttk.Frame(f)
        vk_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=22, pady=2)

        ttk.Label(vk_frame, text="Режим:").grid(row=0, column=0, sticky='w', padx=2)
        cmb_vk = ttk.Combobox(vk_frame, textvariable=self.v_vk_infra_mode,
                               values=['simple', 'modulated', 'phase', 'maximum'],
                               width=12, state='readonly')
        cmb_vk.current(1)
        cmb_vk.grid(row=0, column=1, padx=5)

        ttk.Label(vk_frame, text="Амплитуда:").grid(row=0, column=2, sticky='w', padx=(20, 2))
        self._spin(vk_frame, self.v_vk_infra_amplitude, 0.05, 0.70, 0.05,
                   fmt='%.2f', width=6).grid(row=0, column=3, padx=2)

        r += 1
        self._desc(f, r, 0,
                   "Подмешивает инфразвуковую синусоиду 10-20 Гц к аудиосигналу, меняя форму волны "
                   "и акустический отпечаток. Амплитуда 0.30-0.45 безопасна; выше 0.5 - клиппинг.")
        r += 1

        for v in (self.v_spectral_mask_sens, self.v_spectral_mask_att, self.v_midside_mid,
                  self.v_midside_side, self.v_psycho_intensity, self.v_saturation_drive,
                  self.v_saturation_mix, self.v_jitter_intensity, self.v_jitter_freq,
                  self.v_vk_infra_amplitude):
            v.trace_add('write', lambda *a: self._check_conflicts())

        for v in (self.v_vk_infra, self.v_vk_infra_freq, self.v_vk_infra_amplitude,
                self.v_vk_infra_mode, self.v_vk_infra_mod_freq, self.v_vk_infra_mod_depth,
                self.v_vk_infra_phase_shift, self.v_vk_infra_waveform, self.v_vk_infra_adaptive,
                self.v_vk_infra_h1, self.v_vk_infra_h2, self.v_vk_infra_h3):
            v.trace_add('write', lambda *a: self._schedule_preview_update())

    def _build_advanced_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Дополнительные")

        r = 0
        ttk.Checkbutton(f, text="Обрезать начало (Trim)", variable=self.v_trim,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_trim_val, 0.1, 10.0, 0.1).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="сек").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Удаляет первые N секунд трека. Убирает интро или вступительную тишину, "
                             "делая отпечаток несравнимым с оригиналом.")

        r += 1
        ttk.Checkbutton(f, text="Вырезать фрагмент (Cut Fragment)", variable=self.v_cut,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        sub_c = ttk.Frame(f)
        sub_c.grid(row=r, column=1, columnspan=3, sticky='w', pady=(4, 0))
        ttk.Label(sub_c, text="Позиция:").pack(side='left')
        self._spin(sub_c, self.v_cut_pos, 0, 100, 1, width=5, fmt=None).pack(side='left', padx=2)
        ttk.Label(sub_c, text="%  Длина:").pack(side='left')
        self._spin(sub_c, self.v_cut_dur, 0.1, 30.0, 0.1).pack(side='left', padx=2)
        ttk.Label(sub_c, text="сек").pack(side='left')
        r += 1
        self._desc(f, r, 0, "Вырезает N секунд из указанной позиции, склеивая оставшиеся части. "
                             "Меняет длительность и хронологический отпечаток трека.")

        r += 1
        ttk.Checkbutton(f, text="Плавное затухание в конце (Fade Out)", variable=self.v_fade,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self._spin(f, self.v_fade_val, 0.5, 10.0, 0.5).grid(row=r, column=1, padx=4, pady=(4, 0))
        ttk.Label(f, text="сек").grid(row=r, column=2, sticky='w', pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Плавно снижает громкость в последних N секундах. "
                             "Меняет амплитудный профиль конца трека. Не совмещать с Silent Pad.")

        r += 1
        ttk.Checkbutton(f, text="Сращивание треков (Merge)", variable=self.v_merge,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        sub_m = ttk.Frame(f)
        sub_m.grid(row=r, column=1, columnspan=3, sticky='ew', pady=(4, 0))
        self.entry_extra = ttk.Entry(sub_m, textvariable=self.v_extra, width=28)
        self.entry_extra.pack(side='left', padx=2)
        ttk.Button(sub_m, text="...", width=3, command=self._select_extra_track).pack(side='left')
        r += 1
        self._desc(f, r, 0, "Склеивает текущий трек с выбранным файлом, нормализуя оба до 44100 Гц PCM. "
                             "Результат имеет другую длительность, хэш и акустический профиль.")

        r += 1
        ttk.Checkbutton(f, text="Подмена метаданных длительности (Broken Duration)",
                        variable=self.v_broken,
                        command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
        self.cmb_broken = ttk.Combobox(f, width=22, state='readonly',
                                       values=['Раздуть (x50-200, часы)',
                                               'Обнулить (0.1-3 сек)',
                                               'Случайная (1 мин - 2 ч)',
                                               'Максимум (~4660 ч)'])
        self.cmb_broken.current(0)
        self.cmb_broken.grid(row=r, column=1, columnspan=2, padx=4, pady=(4, 0))
        r += 1
        self._desc(f, r, 0, "Записывает ложную длительность в метаданные файла. "
                             "Плеер будет показывать неверное время, само аудио воспроизводится без изменений.")

        self.v_merge.trace_add('write', lambda *a: self._check_conflicts())
        self.v_extra.trace_add('write', lambda *a: self._check_conflicts())

    def _build_technical_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Технические")

        tech_items = [
            ("Рандомизация битрейта (Bitrate Jitter)",
             self.v_bitrate_j,
             "Кодирует с случайным постоянным битрейтом из [192/224/256/320 кбит/с]. "
             "Меняет размер файла и его акустическую статистику."),
            ("Удаление заголовка Xing (Frame Shift)",
             self.v_frame_sh,
             "Убирает Xing-заголовок из VBR-файла, нарушая навигацию в некоторых плеерах "
             "и делая структуру нестандартной."),
            ("Мусор в поле comment (Fake Metadata)",
             self.v_fake_meta,
             "Вставляет 100-500 случайных символов в поле comment, делая файл "
             "уникальным по тегам и меняя хэш метаданных."),
            ("Переупорядочить ID3 теги (Reorder Tags)",
             self.v_reorder,
             "Переупорядочивает блок ID3v2.3 тегов, меняя их структуру и смещения внутри файла. "
             "Аудиоданные не затрагиваются."),
        ]

        r = 0
        for title, var, desc in tech_items:
            ttk.Checkbutton(f, text=title, variable=var,
                            command=self._check_conflicts).grid(row=r, column=0, sticky='w', padx=4, pady=(4, 0))
            r += 1
            self._desc(f, r, 0, desc)
            r += 1

    def _build_system_tab(self, nb):
        f = ttk.Frame(nb, padding=6)
        nb.add(f, text="Системные")

        cpu_count = os.cpu_count() or 4

        r = 0
        ttk.Label(f, text="Параллельных потоков:", font=('', 9, 'bold')).grid(
            row=r, column=0, sticky='w', padx=4, pady=(8, 2))
        r += 1

        thread_frame = ttk.Frame(f)
        thread_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=4, pady=2)
        self._spin(thread_frame, self.v_max_workers, 1, min(16, cpu_count * 2), 1,
                   width=5, fmt=None).pack(side='left', padx=2)
        ttk.Button(thread_frame, text=f"Авто ({cpu_count})",
                   command=lambda: self.v_max_workers.set(cpu_count)).pack(side='left', padx=8)
        ttk.Label(thread_frame, text=f"CPU: {cpu_count} ядер").pack(side='left', padx=4)

        r += 1
        self._desc(f, r, 0, "Рекомендуется: 2-4 потока для HDD, 4-8 для SSD. "
                             "Слишком много потоков замедляет FFmpeg из-за конкуренции за диск.")

        r += 1
        ttk.Label(f, text="Задержка между запусками (сек):", font=('', 9)).grid(
            row=r, column=0, sticky='w', padx=4, pady=(8, 2))
        r += 1

        delay_frame = ttk.Frame(f)
        delay_frame.grid(row=r, column=0, columnspan=4, sticky='w', padx=4, pady=2)
        self._spin(delay_frame, self.v_thread_delay, 0.0, 5.0, 0.1, width=6).pack(side='left', padx=2)
        ttk.Label(delay_frame, text="сек (0 = без задержки)").pack(side='left', padx=4)

        r += 1
        self._desc(f, r, 0, "Небольшая задержка (0.1-0.5 сек) снижает нагрузку на CPU при старте задач.")

        r += 1
        ttk.Separator(f, orient='horizontal').grid(row=r, column=0, columnspan=4, sticky='ew', pady=8)

        r += 1
        ttk.Label(f, text="Drag & Drop:", font=('', 9, 'bold')).grid(
            row=r, column=0, sticky='w', padx=4)
        r += 1
        dnd_text = "доступен (tkinterdnd2 установлен)" if _DND_AVAILABLE else \
                   "недоступен -> pip install tkinterdnd2"
        dnd_color = '#007700' if _DND_AVAILABLE else '#aa6600'
        ttk.Label(f, text=dnd_text, foreground=dnd_color).grid(
            row=r, column=0, sticky='w', padx=22)

        r += 1
        ttk.Label(f, text="Горячие клавиши:", font=('', 9, 'bold')).grid(
            row=r, column=0, sticky='w', padx=4, pady=(8, 2))
        r += 1
        keys_text = ("Ctrl+O - открыть файлы  |  Ctrl+A - выделить все  |  Delete - удалить файл\n"
                     "Ctrl+C - копировать имена файлов  |  Ctrl+S - сохранить пресет")
        ttk.Label(f, text=keys_text, foreground='#555', font=('', 8)).grid(
            row=r, column=0, columnspan=4, sticky='w', padx=22)

    def _build_output_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Настройки вывода", padding=6)
        lf.pack(side='left', fill='both', expand=True, padx=(0, 4))

        dir_row = ttk.Frame(lf)
        dir_row.pack(fill='x', pady=2)
        ttk.Button(dir_row, text="Выбрать папку", command=self._select_output_dir).pack(side='left')
        self.lbl_out_dir = ttk.Label(dir_row, text=self.output_dir, relief='sunken',
                                     padding=2, width=30)
        self.lbl_out_dir.pack(side='left', padx=4, fill='x', expand=True)

        ttk.Checkbutton(lf, text="Сохранить оригинальные теги",   variable=self.v_preserve_meta).pack(anchor='w')
        ttk.Checkbutton(lf, text="Сохранить оригинальную обложку",variable=self.v_preserve_cover).pack(anchor='w')
        ttk.Checkbutton(lf, text="Удалять оригиналы после обработки", variable=self.v_delete_orig).pack(anchor='w')

        tpl_frame = ttk.Frame(lf)
        tpl_frame.pack(fill='x', pady=(6, 0))
        ttk.Label(tpl_frame, text="Шаблон имени:").pack(side='left')
        _PRESETS = [
            'VK_{n:03d}_custom',
            'modified_{original}',
            '{artist} - {title}',
            '{title}',
            '{original}_{n:03d}',
        ]
        cmb = ttk.Combobox(tpl_frame, textvariable=self.v_filename_template,
                           values=_PRESETS, width=22)
        cmb.pack(side='left', padx=4)
        ttk.Label(tpl_frame, text="?", foreground='#888',
                  cursor='question_arrow').pack(side='left')
        self.v_filename_template.trace_add('write', lambda *_: self._update_name_preview())

        hint = ttk.Label(lf, text="{n} - номер  {original} - исх. имя  {title} {artist} {album} {year}",
                         foreground='#666', font=('', 7))
        hint.pack(anchor='w')

        self.lbl_file_preview = ttk.Label(lf, text="Предпросмотр: -",
                                          foreground='#888', font=('', 8))
        self.lbl_file_preview.pack(anchor='w', pady=(0, 4))

        q_row = ttk.Frame(lf)
        q_row.pack(fill='x', pady=4)
        ttk.Label(q_row, text="Качество:").pack(side='left')
        ttk.Combobox(q_row, textvariable=self.v_quality, width=20, state='readonly',
                     values=['320 kbps (CBR)', '245 kbps (VBR Q0)', '175 kbps (VBR Q4)',
                             '130 kbps (VBR Q6)']
                     ).pack(side='left', padx=4)

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
        ttk.Button(btn_row, text="Импорт",  command=self._import_preset).pack(side='left', padx=2)

        sb2 = ttk.Scrollbar(lf, orient='vertical')
        self.preset_listbox = tk.Listbox(lf, yscrollcommand=sb2.set, height=5, width=28,
                                         exportselection=False)
        sb2.config(command=self.preset_listbox.yview)
        sb2.pack(side='right', fill='y')
        self.preset_listbox.pack(fill='both', expand=True)

        btns = ttk.Frame(lf)
        btns.pack(fill='x', pady=2)
        ttk.Button(btns, text="Загрузить", command=self._load_selected_preset).pack(side='left', padx=2)
        ttk.Button(btns, text="Удалить",   command=self._delete_selected_preset).pack(side='left', padx=2)

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
        self.log_text = scrolledtext.ScrolledText(lf, height=7, state='disabled',
                                                  font=('Courier', 9), wrap='word')
        self.log_text.pack(fill='both', expand=True)
        self.log_text.tag_config('info',    foreground='#333333')
        self.log_text.tag_config('success', foreground='#007700')
        self.log_text.tag_config('warning', foreground='#aa6600')
        self.log_text.tag_config('error',   foreground='#cc0000')

    _TEXT_CLASSES = frozenset(('Entry', 'TEntry', 'Text', 'TCombobox', 'TSpinbox'))

    def _setup_hotkeys(self):
        def _guard(cmd):
            def handler(e):
                if e.widget.winfo_class() in self._TEXT_CLASSES:
                    return
                cmd()
                return 'break'
            return handler

        self.root.bind('<Control-o>', _guard(self._add_files_dialog))
        self.root.bind('<Control-O>', _guard(self._add_files_dialog))
        self.root.bind('<Control-s>', _guard(self._save_preset))
        self.root.bind('<Control-S>', _guard(self._save_preset))

        self.file_listbox.bind('<Control-a>', self._listbox_select_all)
        self.file_listbox.bind('<Control-A>', self._listbox_select_all)
        self.file_listbox.bind('<Delete>', lambda e: self._remove_selected())
        self.file_listbox.bind('<Control-c>', self._listbox_copy)
        self.file_listbox.bind('<Control-C>', self._listbox_copy)

        def _e_copy(e):
            try:
                e.widget.clipboard_clear()
                e.widget.clipboard_append(e.widget.selection_get())
            except Exception:
                pass
            return 'break'

        def _e_paste(e):
            try:
                text = e.widget.clipboard_get()
            except Exception:
                return 'break'
            try:
                e.widget.delete('sel.first', 'sel.last')
            except Exception:
                pass
            try:
                e.widget.insert('insert', text)
            except Exception:
                pass
            return 'break'

        def _e_cut(e):
            try:
                if e.widget.selection_present():
                    e.widget.clipboard_clear()
                    e.widget.clipboard_append(e.widget.selection_get())
                    e.widget.delete('sel.first', 'sel.last')
            except Exception:
                pass
            return 'break'

        def _e_sel_all(e):
            try:
                e.widget.select_range(0, 'end')
                e.widget.icursor('end')
            except Exception:
                pass
            return 'break'

        def _walk_and_bind(widget):
            cls = widget.winfo_class()
            if cls in ('TEntry', 'Entry'):
                widget.bind('<Control-c>', _e_copy)
                widget.bind('<Control-C>', _e_copy)
                widget.bind('<Control-v>', _e_paste)
                widget.bind('<Control-V>', _e_paste)
                widget.bind('<Control-x>', _e_cut)
                widget.bind('<Control-X>', _e_cut)
                widget.bind('<Control-a>', _e_sel_all)
                widget.bind('<Control-A>', _e_sel_all)
            for child in widget.winfo_children():
                _walk_and_bind(child)

        _walk_and_bind(self.root)

        def _text_copy(e):
            try:
                e.widget.clipboard_clear()
                e.widget.clipboard_append(e.widget.get('sel.first', 'sel.last'))
            except Exception:
                pass
            return 'break'

        self.log_text.bind('<Control-c>', _text_copy)
        self.log_text.bind('<Control-C>', _text_copy)
        self.conv_log_text.bind('<Control-c>', _text_copy)
        self.conv_log_text.bind('<Control-C>', _text_copy)

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
        except:
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
            warns.append("Spectral Masking + Spectral Jitter: множественные провалы, возможны артефакты")
        if self.v_concert_emu.get() and self.v_midside.get():
            warns.append("Concert Emulation + Mid/Side: оба меняют стереокартину, может быть избыточно")
        if self.v_saturation.get() and self.v_concert_emu.get():
            warns.append("Saturation + Concert: компрессия и искажения могут суммироваться")
        if self.v_vk_infra.get() and self.v_ultra.get():
            warns.append("VK Инфразвук + Ultrasonic: двойная обработка вне слышимого диапазона")
        if self.v_temp_jitter.get() and (self.v_pitch.get() or self.v_speed.get()):
            warns.append("Temporal Jitter + Pitch/Speed: множественные изменения времени")
        if self.v_resamp.get() and (self.v_pitch.get() or self.v_speed.get()):
            warns.append("Resample Drift + Pitch/Speed: каскадные артефакты")
        if self.v_phase_inv.get() and self.v_phase_scr.get():
            warns.append("Phase Invert + Scramble: взаимное подавление фазы")
        if self.v_cut.get() and self.v_trim.get():
            warns.append("Cut + Trim: трек может стать слишком коротким")
        if self.v_ultra.get() and self.v_dither.get():
            warns.append("Ultrasonic + Dither: избыточный спектральный шум")

        try:
            if self.v_vk_infra.get() and self.v_vk_infra_amplitude.get() > 0.5:
                warns.append(f"VK Инфразвук: амплитуда {self.v_vk_infra_amplitude.get():.2f} > 0.5 - возможен клиппинг")
            if self.v_pitch.get() and abs(self.v_pitch_val.get()) > 5.0:
                warns.append(f"Pitch: ±{self.v_pitch_val.get():.1f} - очень большое значение, будет слышно")
            if self.v_speed.get():
                spd = self.v_speed_val.get()
                if spd < 0.90 or spd > 1.10:
                    warns.append(f"Speed: {spd:.2f}x - может быть слышно")
            if self.v_silence.get() and self.v_silence_val.get() > 120:
                warns.append(f"Тишина: {self.v_silence_val.get()} сек - очень много")
            if self.v_phase_scr.get() and self.v_phase_scr_val.get() > 4.0:
                warns.append(f"Phase Scramble: {self.v_phase_scr_val.get()} Гц - может быть слышно")
            if self.v_resamp.get() and abs(self.v_resamp_val.get()) > 7:
                warns.append(f"Resample Drift: ±{abs(self.v_resamp_val.get())} Гц - может быть слышно")
            if self.v_ultra.get() and self.v_ultra_level.get() > 0.008:
                warns.append(f"Ultrasonic level {self.v_ultra_level.get():.4f} - может быть слышно")
            if self.v_spectral_mask.get() and self.v_spectral_mask_att.get() > 20:
                warns.append(f"Spectral Mask: {self.v_spectral_mask_att.get()} dB - возможны артефакты")
        except tk.TclError:
            pass

        self.lbl_conflict.config(text="\n".join(f"WARN: {w}" for w in warns) if warns else "")
        self.btn_start.config(state='normal')
        self._schedule_preview_update()

    def _add_files_dialog(self):
        if self._mode == 'converter':
            files = filedialog.askopenfilenames(title="Выберите аудиофайлы",
                                                filetypes=INPUT_EXTENSIONS)
        else:
            files = filedialog.askopenfilenames(title="Выберите MP3 файлы",
                                                filetypes=[("MP3 files", "*.mp3")])
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
                    f"Обложка: {'есть' if t.cover_data else 'нет'}",
                ]
                self.txt_track_info.insert('end', "\n".join(lines))
            else:
                self.txt_track_info.insert('end', "Информация недоступна (режим конвертера)")
        self.txt_track_info.config(state='disabled')

    def _cleanup_temp_cover(self):
        if self._cover_is_temp and self.selected_cover_path and os.path.exists(self.selected_cover_path):
            try:
                os.unlink(self.selected_cover_path)
            except:
                pass
        self._cover_is_temp = False

    def _select_cover(self):
        fp = filedialog.askopenfilename(title="Выберите обложку",
                                        filetypes=[("Images", "*.png *.jpg *.jpeg")])
        if fp:
            self._cleanup_temp_cover()
            self.selected_cover_path = fp
            self.lbl_cover.config(text=os.path.basename(fp))
            self.btn_rm_cover.config(state='normal')
            self._log(f"Обложка: {os.path.basename(fp)}", 'success')

    def _random_cover(self):
        try:
            import struct, zlib
            r, g, b = random.randint(50,200), random.randint(50,200), random.randint(50,200)
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
            self.v_title.set(t.title); self.v_artist.set(t.artist)
            self.v_album.set(t.album); self.v_year.set(t.year); self.v_genre.set(t.genre)
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
        fp = filedialog.askopenfilename(title="Дополнительный трек",
                                        filetypes=INPUT_EXTENSIONS)
        if fp:
            self.v_extra.set(fp)

    def _save_preset(self):
        name = self.v_preset_name.get().strip() or f"Preset {len(self.saved_presets)+1}"
        data = {'name': name, 'date': datetime.now().isoformat(),
                'settings': self._collect_settings()}
        self.saved_presets.append(data)
        self._refresh_preset_list()
        self.v_preset_name.set('')
        self._save_config()
        self._log(f"Пресет '{name}' сохранён", 'success')

    def _refresh_preset_list(self):
        self.preset_listbox.delete(0, 'end')
        for p in self.saved_presets:
            self.preset_listbox.insert('end', p.get('name', '?'))
        if hasattr(self, '_quick_presets_frame'):
            self._refresh_quick_presets()

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
        self.v_spectral_mask_sens.set(s.get('spectral_mask_sensitivity', 0.8))
        self.v_spectral_mask_att.set(s.get('spectral_mask_attenuation', 12))
        self.v_spectral_mask_peaks.set(s.get('spectral_mask_peaks', 10))
        self.v_concert_intensity.set(s.get('concert_intensity', 'medium'))
        self.v_midside_mid.set(s.get('midside_mid_gain', -3.0))
        self.v_midside_side.set(s.get('midside_side_gain', 2.0))
        self.v_psycho_intensity.set(s.get('psychoacoustic_intensity', 0.0003))
        self.v_saturation_drive.set(s.get('saturation_drive', 1.5))
        self.v_saturation_mix.set(s.get('saturation_mix', 0.15))
        self.v_jitter_intensity.set(s.get('jitter_intensity', 0.002))
        self.v_jitter_freq.set(s.get('jitter_frequency', 0.5))
        self.v_spec_jitter_count.set(s.get('spectral_jitter_count', 5))
        self.v_spec_jitter_att.set(s.get('spectral_jitter_attenuation', 15))
        self.v_vk_infra.set(methods.get('vk_infrasonic', False))
        self.v_vk_infra_mode.set(s.get('vk_infrasonic_mode', 'modulated'))
        self.v_vk_infra_amplitude.set(s.get('vk_infrasonic_amplitude', 0.35))
        self.v_vk_infra_freq.set(s.get('vk_infrasonic_freq', 18.0))
        self.v_vk_infra_mod_freq.set(s.get('vk_infrasonic_mod_freq', 0.08))
        self.v_vk_infra_mod_depth.set(s.get('vk_infrasonic_mod_depth', 0.3))
        self.v_vk_infra_phase_shift.set(s.get('vk_infrasonic_phase_shift', 0.0))
        self.v_vk_infra_waveform.set(s.get('vk_infrasonic_waveform', 'sine'))
        self.v_vk_infra_adaptive.set(s.get('vk_infrasonic_adaptive', True))
        self.v_vk_infra_h1.set(s.get('vk_infrasonic_h1', 0.15))
        self.v_vk_infra_h2.set(s.get('vk_infrasonic_h2', 0.07))
        self.v_vk_infra_h3.set(s.get('vk_infrasonic_h3', 0.03))
        self.v_pitch_val.set(s.get('pitch_value', 0.5))
        self.v_speed_val.set(s.get('speed_value', 1.00))
        self.v_eq_val.set(s.get('eq_value', -2.0))
        self.v_silence_val.set(s.get('silence_duration', 45))
        self.v_phase_inv_val.set(s.get('phase_invert_strength', 1.0))
        self.v_phase_scr_val.set(s.get('phase_scramble_speed', 2.0))
        self.v_dc_val.set(s.get('dc_shift_value', 0.000005))
        self.v_resamp_val.set(s.get('resample_drift_amount', 1))
        self.v_ultra_freq.set(s.get('ultrasonic_freq', 21000))
        self.v_ultra_level.set(s.get('ultrasonic_level', 0.001))
        self.v_haas_val.set(s.get('haas_delay_ms', 15.0))
        self.v_dither_method.set(s.get('dither_method', 'triangular_hp'))
        self.v_id3pad_val.set(s.get('id3_padding_bytes', 512))
        self.v_trim_val.set(s.get('trim_duration', 5.0))
        self.v_cut_pos.set(s.get('cut_position_percent', 50))
        self.v_cut_dur.set(s.get('cut_duration', 2.0))
        self.v_fade_val.set(s.get('fade_duration', 5.0))
        if hasattr(self, 'cmb_eq_type'):
            self.cmb_eq_type.current(s.get('eq_type', 0))
        if hasattr(self, 'cmb_broken'):
            self.cmb_broken.current(s.get('broken_type', 0))
        _Q_DISPLAY = {
            '320k': '320 kbps (CBR)',
            '0':    '245 kbps (VBR Q0)',
            '4':    '175 kbps (VBR Q4)',
            '6':    '130 kbps (VBR Q6)',
        }
        self.v_quality.set(_Q_DISPLAY.get(str(s.get('quality', '0')), '245 kbps (VBR Q0)'))

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
        fp = filedialog.asksaveasfilename(defaultextension='.json',
                                          filetypes=[("JSON", "*.json")])
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
                'pitch': self.v_pitch.get(), 'speed': self.v_speed.get(),
                'eq': self.v_eq.get(), 'silence': self.v_silence.get(),
                'phase_invert': self.v_phase_inv.get(), 'phase_scramble': self.v_phase_scr.get(),
                'dc_shift': self.v_dc.get(), 'resample_drift': self.v_resamp.get(),
                'ultrasonic_noise': self.v_ultra.get(), 'haas_delay': self.v_haas.get(),
                'dither_attack': self.v_dither.get(), 'id3_padding_attack': self.v_id3pad.get(),
                'trim_silence': self.v_trim.get(), 'cut_fragment': self.v_cut.get(),
                'fade_out': self.v_fade.get(), 'merge': self.v_merge.get(),
                'broken_duration': self.v_broken.get(), 'bitrate_jitter': self.v_bitrate_j.get(),
                'frame_shift': self.v_frame_sh.get(), 'fake_metadata': self.v_fake_meta.get(),
                'reorder_tags': self.v_reorder.get(),
                'spectral_masking': self.v_spectral_mask.get(),
                'concert_emulation': self.v_concert_emu.get(),
                'midside_processing': self.v_midside.get(),
                'psychoacoustic_noise': self.v_psycho_noise.get(),
                'saturation': self.v_saturation.get(),
                'temporal_jitter': self.v_temp_jitter.get(),
                'spectral_jitter': self.v_spec_jitter.get(),
                'vk_infrasonic': self.v_vk_infra.get(),
            },
            'pitch_value':          self.v_pitch_val.get(),
            'speed_value':          self.v_speed_val.get(),
            'eq_value':             self.v_eq_val.get(),
            'eq_type':              eq_type_idx,
            'silence_duration':     self.v_silence_val.get(),
            'phase_invert_strength':self.v_phase_inv_val.get(),
            'phase_scramble_speed': self.v_phase_scr_val.get(),
            'dc_shift_value':       self.v_dc_val.get(),
            'resample_drift_amount':self.v_resamp_val.get(),
            'ultrasonic_freq':      self.v_ultra_freq.get(),
            'ultrasonic_level':     self.v_ultra_level.get(),
            'haas_delay_ms':        self.v_haas_val.get(),
            'dither_method':        self.v_dither_method.get(),
            'id3_padding_bytes':    self.v_id3pad_val.get(),
            'trim_duration':        self.v_trim_val.get(),
            'cut_position_percent': self.v_cut_pos.get(),
            'cut_duration':         self.v_cut_dur.get(),
            'fade_duration':        self.v_fade_val.get(),
            'extra_track_path':     self.v_extra.get(),
            'broken_type':          broken_type_idx,
            'spectral_mask_sensitivity': self.v_spectral_mask_sens.get(),
            'spectral_mask_attenuation': self.v_spectral_mask_att.get(),
            'spectral_mask_peaks':       self.v_spectral_mask_peaks.get(),
            'concert_intensity':         self.v_concert_intensity.get(),
            'midside_mid_gain':          self.v_midside_mid.get(),
            'midside_side_gain':         self.v_midside_side.get(),
            'psychoacoustic_intensity':  self.v_psycho_intensity.get(),
            'saturation_drive':          self.v_saturation_drive.get(),
            'saturation_mix':            self.v_saturation_mix.get(),
            'jitter_intensity':          self.v_jitter_intensity.get(),
            'jitter_frequency':          self.v_jitter_freq.get(),
            'spectral_jitter_count':     self.v_spec_jitter_count.get(),
            'spectral_jitter_attenuation': self.v_spec_jitter_att.get(),
            'vk_infrasonic_mode':        self.v_vk_infra_mode.get(),
            'vk_infrasonic_amplitude':   self.v_vk_infra_amplitude.get(),
            'vk_infrasonic_freq':        self.v_vk_infra_freq.get(),
            'vk_infrasonic_mod_freq':    self.v_vk_infra_mod_freq.get(),
            'vk_infrasonic_mod_depth':   self.v_vk_infra_mod_depth.get(),
            'vk_infrasonic_phase_shift': self.v_vk_infra_phase_shift.get(),
            'vk_infrasonic_waveform':    self.v_vk_infra_waveform.get(),
            'vk_infrasonic_adaptive':    self.v_vk_infra_adaptive.get(),
            'vk_infrasonic_h1':          self.v_vk_infra_h1.get(),
            'vk_infrasonic_h2':          self.v_vk_infra_h2.get(),
            'vk_infrasonic_h3':          self.v_vk_infra_h3.get(),
            'selected_cover_path':  self.selected_cover_path,
            'quality':              quality_map[q_idx],
            'preserve_metadata':    self.v_preserve_meta.get(),
            'preserve_cover':       self.v_preserve_cover.get(),
            'rename_files':         False,
            'delete_original':      self.v_delete_orig.get(),
            'reupload':             self.v_reupload.get(),
            'reupload_text':        self.v_reupload_text.get(),
            'reupload_pos':         self.v_reupload_pos.get(),
            'filename_template':    self.v_filename_template.get() or 'VK_{n:03d}_custom',
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
            'album': self.v_album.get(), 'year': self.v_year.get(),
            'genre': self.v_genre.get(),
        }

        max_workers = self.v_max_workers.get()
        self._log(f"Запущена обработка ({max_workers} поток(а))...", 'info')

        if max_workers > 1:
            processor = BatchProcessor(
                files=list(self.input_files),
                tracks_info=list(self.tracks_info),
                output_dir=self.output_dir,
                settings=settings,
                metadata=metadata,
                result_queue=self._worker_queue,
                max_workers=max_workers,
                delay_between=self.v_thread_delay.get(),
            )
            processor.run_in_thread()
        else:
            worker = ModificationWorker(
                files=list(self.input_files),
                tracks_info=list(self.tracks_info),
                output_dir=self.output_dir,
                settings=settings,
                metadata=metadata,
                on_progress=lambda cur, tot, fp: self._worker_queue.put(('progress', cur, tot, fp)),
                on_file_complete=lambda fp, ok, out: self._worker_queue.put(('file_done', fp, ok, out)),
                on_all_complete=lambda sc, tot: self._worker_queue.put(('all_done', sc, tot)),
                on_error=lambda msg: self._worker_queue.put(('error', msg)),
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

    def _load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                self.output_dir = cfg.get('output_dir', self.output_dir) or self.output_dir
                self.saved_presets = cfg.get('presets', [])
        except:
            pass

    def _save_config(self):
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({'output_dir': self.output_dir, 'presets': self.saved_presets},
                          f, indent=2, ensure_ascii=False)
        except:
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
            files=[file_path],
            tracks_info=[track_info],
            output_dir=self.output_dir,
            settings=self.settings,
            metadata=self.metadata,
            on_progress=lambda *a: None,
            on_file_complete=_on_done,
            on_all_complete=lambda *a: None,
            on_error=lambda msg: self.queue.put(('error', msg)),
            start_index=idx,
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
        result = [result[i] * (1.0 + 0.15 * math.sin(2 * math.pi * speed * i / sr))
                  for i in range(n)]

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
        if eq_type == 1:
            gain = 10 ** (-4 / 20)
        elif eq_type == 2:
            gain = 10 ** (3 / 20)
        else:
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
        thresh = {'light': 0.7, 'medium': 0.55, 'heavy': 0.40}.get(intensity, 0.55)
        for i in range(n):
            v = result2[i]
            if abs(v) > thresh:
                excess = abs(v) - thresh
                v = math.copysign(thresh + excess * 0.25, v)
            result2[i] = v
        result = result2

    if s.get('midside', False):
        mid_g = 10 ** (s.get('midside_mid', -3.0) / 40)
        side_g = 10 ** (s.get('midside_side', 2.0) / 40)
        blend = (mid_g + side_g) / 2
        result = [v * blend for v in result]

    if s.get('psycho', False) and n > 0:
        intensity = s.get('psycho_intensity', 0.0003)
        result = [result[i] + intensity * math.sin(i * 7.3 + math.cos(i * 3.7))
                  for i in range(n)]

    if s.get('ultra', False) and n > 0:
        level = s.get('ultra_level', 0.001)
        ultra_preview_freq = 80.0
        result = [result[i] + level * math.sin(2 * math.pi * ultra_preview_freq * i / sr)
                  for i in range(n)]

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