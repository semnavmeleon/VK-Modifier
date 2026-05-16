import hashlib
import math
import os
import random
import subprocess
import tempfile
import threading
import time
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, TCON, APIC, TLEN, TXXX, PRIV

FORMAT_CODECS = {
    'mp3': 'libmp3lame', 'flac': 'flac', 'wav': 'pcm_s16le', 'ogg': 'libvorbis',
    'aac': 'aac', 'm4a': 'aac', 'wma': 'wmav2', 'opus': 'libopus',
    'aiff': 'pcm_s16be', 'alac': 'alac', 'wv': 'wavpack', 'ape': 'ape',
    'tta': 'tta', 'ac3': 'ac3', 'dts': 'dts', 'mp2': 'mp2', 'mpc': 'mpc',
    'spx': 'libspeex', 'amr': 'libopencore_amrnb', 'au': 'pcm_s16be',
    'mka': 'libvorbis', 'oga': 'flac', 'caf': 'pcm_s16le', 'shn': 'shorten',
}


class TrackInfo:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.size_mb = os.path.getsize(file_path) / (1024 * 1024)
        self.duration_sec = 0
        self.title = ""
        self.artist = ""
        self.album = ""
        self.year = ""
        self.genre = ""
        self.cover_data = None
        self.cover_mime = "image/jpeg"
        self.bitrate = 0
        self.sample_rate = 0
        self.file_hash = ""
        self._load_metadata()

    def _load_metadata(self):
        try:
            audio = MP3(self.file_path)
            self.duration_sec = audio.info.length
            self.bitrate = audio.info.bitrate // 1000
            self.sample_rate = audio.info.sample_rate
        except Exception:
            pass
        try:
            tags = ID3(self.file_path)
            self.title = str(tags.get('TIT2', ''))
            self.artist = str(tags.get('TPE1', ''))
            self.album = str(tags.get('TALB', ''))
            self.year = str(tags.get('TDRC', ''))
            self.genre = str(tags.get('TCON', ''))
            for key in tags:
                if key.startswith('APIC'):
                    self.cover_data = tags[key].data
                    self.cover_mime = tags[key].mime
                    break
        except Exception:
            pass


class BatchProcessor:
    def __init__(self, files, tracks_info, output_dir, settings, metadata,
                 result_queue, max_workers=4, delay_between=0.0, stop_event=None):
        self.files = files
        self.tracks_info = tracks_info
        self.output_dir = output_dir
        self.settings = settings
        self.metadata = metadata
        self.queue = result_queue
        self.max_workers = max_workers
        self.delay_between = delay_between
        self._stop_event = stop_event or threading.Event()
        self._success_count = 0
        self._lock = threading.Lock()

    def run_in_thread(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _process_one(self, idx, file_path, track_info):
        if self._stop_event.is_set():
            return

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
            start_index=idx,
            stop_event=self._stop_event
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
                m = re.search(r'\b(\d+)\b', self.quality_preset)
                bitrate = m.group(1) if m else '320'
                args.extend(['-codec:a', codec, '-b:a', f'{bitrate}k'])
            else:
                q_match = re.search(r'Q(\d)', self.quality_preset)
                q = q_match.group(1) if q_match else '0'
                args.extend(['-codec:a', codec, '-q:a', q])
        elif self.output_format in ['aac', 'm4a', 'opus', 'wma']:
            m = re.search(r'\b(\d+)\b', self.quality_preset)
            bitrate = m.group(1) if m else '320'
            args.extend(['-codec:a', codec, '-b:a', f'{bitrate}k'])
        elif self.output_format == 'ogg':
            q_match = re.search(r'\b(10|[0-9])\b', self.quality_preset)
            q = q_match.group(1) if q_match else '6'
            args.extend(['-codec:a', codec, '-q:a', q])
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
            result = subprocess.run(args, capture_output=True, stdin=subprocess.DEVNULL, encoding='utf-8', errors='ignore', timeout=300)

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


class ModificationWorker(threading.Thread):
    def __init__(self, files, tracks_info, output_dir, settings, metadata,
                 on_progress=None, on_file_complete=None, on_all_complete=None, on_error=None,
                 start_index=0, stop_event=None):
        super().__init__(daemon=True)
        self.files = files
        self.tracks_info = tracks_info
        self.output_dir = output_dir
        self.settings = settings
        self.metadata = metadata
        self.on_progress = on_progress or (lambda *a: None)
        self.on_file_complete = on_file_complete or (lambda *a: None)
        self.on_all_complete = on_all_complete or (lambda *a: None)
        self.on_error = on_error or (lambda *a: None)
        self.start_index = start_index
        self._stop_event = stop_event or threading.Event()

    @staticmethod
    def _safe_filename(s):
        return re.sub(r'[\\/*?:"<>|]', '_', str(s)).strip() or '_'

    @staticmethod
    def _extract_ffmpeg_error(stderr, chars=2000):
        lines = stderr.splitlines()
        error_lines = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if any(stripped.startswith(p) for p in (
                'ffmpeg version', 'built with', 'configuration:', 'lib', 'Copyright'
            )):
                continue
            error_lines.append(line)
        result_text = '\n'.join(error_lines)
        return result_text[-chars:] if len(result_text) > chars else result_text

    def _safe_subprocess_run(self, cmd, description="", allow_fail=False):
        try:
            result = subprocess.run(cmd, capture_output=True, stdin=subprocess.DEVNULL, encoding='utf-8', errors='ignore', timeout=300)
            if result.returncode != 0 and not allow_fail:
                err = self._extract_ffmpeg_error(result.stderr)
                self.on_error(f"FFmpeg ошибка ({description}):\n{err}")
                return False, result
            return True, result
        except subprocess.TimeoutExpired:
            if not allow_fail:
                self.on_error(f"Таймаут операции: {description}")
            return False, None
        except Exception as e:
            if not allow_fail:
                self.on_error(f"Ошибка ({description}): {e}")
            return False, None

    def _try_alternative_merge(self, main_track, extra_track, temp_files, output_path):
        cmd = [
            'ffmpeg', '-i', main_track, '-i', extra_track,
            '-filter_complex', '[0:a][1:a]concat=n=2:v=0:a=1[out]',
            '-map', '[out]', '-codec:a', 'libmp3lame', '-q:a', '2',
            '-y', output_path
        ]
        self._safe_subprocess_run(cmd, "alternative merge", allow_fail=True)

    def _get_duration(self, file_path):
        try:
            cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                   '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
            success, result = self._safe_subprocess_run(cmd, "getting duration", allow_fail=True)
            if success and result.stdout.strip():
                return float(result.stdout.strip())
        except:
            pass
        return 0

    def _verify_mp3(self, file_path):
        try:
            audio = MP3(file_path)
            if audio.info.length > 0:
                return True
        except:
            pass
        cmd = ['ffprobe', '-v', 'error', file_path]
        success, _ = self._safe_subprocess_run(cmd, "verifying mp3", allow_fail=True)
        return success

    def _build_spectral_mask_filter(self, audio_path, num_peaks=12, sensitivity=0.8, attenuation=12):
        bands = [
            40, 50, 63, 80, 100, 125, 160, 200, 250, 315, 400, 500, 630,
            800, 1000, 1250, 1600, 2000, 2500, 3150, 4000, 5000, 6300,
            8000, 10000, 12500, 16000
        ]

        band_energies = self._analyze_spectrum(audio_path, bands)
        if not band_energies:
            return None

        energies = [e for _, e in band_energies]
        avg_energy = sum(energies) / len(energies)
        std_dev = (sum((e - avg_energy) ** 2 for e in energies) / len(energies)) ** 0.5
        threshold = avg_energy + (sensitivity * std_dev)

        peaks = [(f, e) for f, e in band_energies if e > threshold]
        peaks.sort(key=lambda x: x[1], reverse=True)
        peaks = peaks[:num_peaks]

        if not peaks:
            return None

        return ", ".join(
            f"equalizer=f={f}:width_type=q:width=2.0:g=-{attenuation}" for f, _ in peaks
        )

    def _analyze_spectrum(self, audio_path, bands):
        try:
            import numpy as np
            sample_rate = 44100
            cmd = ['ffmpeg', '-i', audio_path, '-vn', '-f', 'f32le',
                   '-ac', '1', '-ar', str(sample_rate), 'pipe:1', '-y']
            result = subprocess.run(cmd, capture_output=True, stdin=subprocess.DEVNULL, timeout=120)
            if result.returncode != 0 or not result.stdout:
                return self._analyze_spectrum_ffmpeg(audio_path, bands)

            samples = np.frombuffer(result.stdout, dtype=np.float32)
            if len(samples) < sample_rate:
                return self._analyze_spectrum_ffmpeg(audio_path, bands)

            samples = samples[:sample_rate * 30]
            fft_size = 1 << (len(samples) - 1).bit_length()
            fft_size = min(fft_size, 1 << 20)
            spectrum = np.abs(np.fft.rfft(samples, n=fft_size)) ** 2
            freqs = np.fft.rfftfreq(fft_size, d=1.0 / sample_rate)

            band_energies = []
            for center in bands:
                low = center / 1.4
                high = center * 1.4
                mask = (freqs >= low) & (freqs <= high)
                if mask.any():
                    rms = float(np.sqrt(np.mean(spectrum[mask])))
                    if rms > 0:
                        rms_db = 10 * np.log10(rms)
                        if rms_db > -80:
                            band_energies.append((center, rms_db))
            return band_energies or None

        except ImportError:
            return self._analyze_spectrum_ffmpeg(audio_path, bands)
        except Exception:
            return self._analyze_spectrum_ffmpeg(audio_path, bands)

    def _analyze_spectrum_ffmpeg(self, audio_path, bands):
        band_energies = []
        for center_freq in bands:
            cmd = [
                'ffmpeg', '-i', audio_path,
                '-af', f'bandpass=f={center_freq}:width={center_freq // 4}:csg=1,astats=measure=RMS',
                '-f', 'null', '-'
            ]
            success, result = self._safe_subprocess_run(cmd, f"analyzing {center_freq}Hz", allow_fail=True)
            if success and result and result.stderr:
                m = re.search(r'RMS level dB:\s*([-\d.]+)', result.stderr)
                if not m:
                    m = re.search(r'RMS level:\s*([-\d.]+)', result.stderr)
                if m:
                    rms_db = float(m.group(1))
                    if rms_db > -80:
                        band_energies.append((center_freq, rms_db))
        return band_energies or None

    def _build_concert_emulation_filter(self, intensity='medium'):
        filters = []

        configs = {
            'light':  {'echo_in': 0.8, 'echo_out': 0.92, 'echo_delay': 40,  'echo_decay': 0.12,
                       'compressor': 1.5, 'eq_low': -1, 'eq_high': -2, 'stereo_w': 0.15},
            'medium': {'echo_in': 0.8, 'echo_out': 0.88, 'echo_delay': 65,  'echo_decay': 0.22,
                       'compressor': 3.0, 'eq_low': -2, 'eq_high': -4, 'stereo_w': 0.30},
            'heavy':  {'echo_in': 0.8, 'echo_out': 0.82, 'echo_delay': 100, 'echo_decay': 0.32,
                       'compressor': 6.0, 'eq_low': -3, 'eq_high': -6, 'stereo_w': 0.45},
        }

        cfg = configs.get(intensity, configs['medium'])

        w = cfg['stereo_w']
        a = round(1 - w / 2, 4)
        b = round(w / 2, 4)
        filters.append(f"pan=stereo|c0={a}*c0+{b}*c1|c1={b}*c0+{a}*c1")

        filters.append(
            f"aecho={cfg['echo_in']}:{cfg['echo_out']}:{cfg['echo_delay']}:{cfg['echo_decay']}"
        )

        filters.append(
            f"acompressor=threshold=-20dB:ratio=3:attack=50:release=200:knee=6:makeup={cfg['compressor']}"
        )

        if cfg['eq_low'] != 0:
            filters.append(f"bass=g={cfg['eq_low']}:f=100:width_type=q:width=0.7")
        if cfg['eq_high'] != 0:
            filters.append(f"treble=g={cfg['eq_high']}:f=8000:width_type=q:width=0.7")

        return ", ".join(filters)
    
    def _build_midside_filter(self, mid_eq_gain=-3, side_eq_gain=2):
        mid_lin  = 10 ** (mid_eq_gain  / 20)
        side_lin = 10 ** (side_eq_gain / 20)
        return f"stereotools=mlev={mid_lin:.4f}:slev={side_lin:.4f}"

    def _build_psychoacoustic_noise_filter(self, intensity=0.0003):
        decay = min(0.01 + intensity * 50, 0.15)
        return f"aecho=0.9:0.9:1:{decay:.3f}"

    def _build_saturation_filter(self, drive=1.5, mix=0.15):
        mix_clamped = max(0.0, min(1.0, mix))
        drive_clamped = max(1.0, min(5.0, drive))
        return (
            f"acompressor=threshold=-12dB:ratio=4:attack=1:release=50:makeup=2,"
            f"acrusher=level_in={drive_clamped:.2f}:level_out=1:bits=14:mode=log:mix={mix_clamped:.2f}"
        )

    def _build_temporal_jitter_filter(self, intensity=0.002, frequency=0.5):
        freq_clamped = max(0.1, min(20.0, frequency))
        delay = max(0.1, min(5.0, intensity * 2000))
        decay = min(0.3, intensity * 50)
        return f"aphaser=type=t:delay={delay:.2f}:decay={decay:.3f}:speed={freq_clamped:.2f}:out_gain=0.9"

    def _build_spectral_jitter_filter(self, num_notches=5, max_attenuation=15, fixed_frequencies=None, 
                                       fixed_attenuation=None, manual_config=None):
        filters = []
        freq_pool = [
            120, 250, 400, 630, 800, 1200, 1600, 2000, 
            2500, 3150, 4000, 5000, 6300, 8000, 10000, 12500
        ]
        
        if manual_config is not None and manual_config.get('mode') == 'manual':
            frequencies = manual_config.get('frequencies', [])
            attenuations = manual_config.get('attenuations', [])
            widths = manual_config.get('widths', [])
            default_width = manual_config.get('fixed_width', 0.2)
            
            for i, freq in enumerate(frequencies):
                att = attenuations[i] if i < len(attenuations) else max_attenuation
                width = widths[i] if i < len(widths) else default_width
                filters.append(f"equalizer=f={freq}:width_type=q:width={width:.3f}:g=-{att}")
            return ", ".join(filters)
        
        if fixed_frequencies is not None and len(fixed_frequencies) > 0:
            selected = fixed_frequencies
            default_width = 0.2
            if manual_config and 'fixed_width' in manual_config:
                default_width = manual_config['fixed_width']
                
            for freq in selected:
                att = fixed_attenuation if fixed_attenuation is not None else max_attenuation
                if manual_config and 'fixed_attenuation' in manual_config:
                    att = manual_config['fixed_attenuation']
                width = default_width
                filters.append(f"equalizer=f={freq}:width_type=q:width={width:.3f}:g=-{att}")
        else:
            num_notches_int = int(round(num_notches))
            if num_notches_int <= 0:
                return ""
            selected = random.sample(freq_pool, min(num_notches_int, len(freq_pool)))
            
            for freq in selected:
                if fixed_attenuation is not None:
                    att = fixed_attenuation
                else:
                    att = random.uniform(max_attenuation / 2, max_attenuation)
                q = random.uniform(1.5, 3.0)
                filters.append(f"equalizer=f={freq}:width_type=q:width={q:.2f}:g=-{att:.1f}")
        
        return ", ".join(filters)

    def _get_vk_infrasonic_expr(self, settings, extra_phase=0.0):
        freq = settings.get('vk_infrasonic_freq', 18.0)
        amp = settings.get('vk_infrasonic_amplitude', 0.35)
        mode = settings.get('vk_infrasonic_mode', 'modulated')
        mod_freq = settings.get('vk_infrasonic_mod_freq', 0.08)
        mod_depth = settings.get('vk_infrasonic_mod_depth', 0.3)
        phase_shift = settings.get('vk_infrasonic_phase_shift', 0.0)
        harmonics = settings.get('vk_infrasonic_harmonics', [0.15, 0.07, 0.03])
        waveform = settings.get('vk_infrasonic_waveform', 'sine')

        def wave_func(wave_type, arg):
            if wave_type == 'sine':
                return f"sin({arg})"
            elif wave_type == 'triangle':
                return f"2/PI*asin(sin({arg}))"
            elif wave_type == 'square':
                return f"if(gte(sin({arg}),0),1.0,-1.0)"
            else:
                return f"sin({arg})"

        base_arg = f"2*PI*{freq}*t"
        total_phase = phase_shift + extra_phase
        if total_phase != 0:
            base_arg = f"({base_arg}+{total_phase:.4f})"

        if mode == 'simple':
            expr = f"{amp}*{wave_func(waveform, base_arg)}"
        elif mode == 'modulated':
            mod_term = f"(1-{mod_depth}+{mod_depth}*sin(2*PI*{mod_freq}*t))"
            expr = f"{amp}*{mod_term}*{wave_func(waveform, base_arg)}"
        elif mode == 'phase':
            phase_mod = f"{mod_depth}*sin(2*PI*{mod_freq}*t)"
            mod_arg = f"({base_arg}+{phase_mod})"
            expr = f"{amp}*{wave_func(waveform, mod_arg)}"
        elif mode == 'harmonic':
            terms = [f"{amp}*{wave_func(waveform, base_arg)}"]
            for i, h_amp in enumerate(harmonics, start=2):
                if h_amp > 0:
                    h_freq = freq * i
                    h_arg = f"2*PI*{h_freq}*t"
                    if total_phase != 0:
                        h_arg = f"({h_arg}+{total_phase:.4f})"
                    terms.append(f"{amp * h_amp}*{wave_func(waveform, h_arg)}")
            expr = "+".join(terms)
        else:
            mod_term = f"(1-{mod_depth}+{mod_depth}*sin(2*PI*{mod_freq}*t))"
            main_term = f"{amp}*{mod_term}*{wave_func(waveform, base_arg)}"
            if harmonics and len(harmonics) > 0 and harmonics[0] > 0:
                h2_amp = harmonics[0]
                h2_arg = f"2*PI*{freq*2}*t"
                if total_phase != 0:
                    h2_arg = f"({h2_arg}+{total_phase:.4f})"
                harm_term = f"{amp * h2_amp}*{mod_term}*{wave_func(waveform, h2_arg)}"
                expr = f"{main_term}+{harm_term}"
            else:
                expr = main_term
        
        return expr

    def _get_sample_rate(self, file_path):
        try:
            cmd = ['ffprobe', '-v', 'error', '-select_streams', 'a:0',
                '-show_entries', 'stream=sample_rate',
                '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
            success, result = self._safe_subprocess_run(cmd, "getting sample rate", allow_fail=True)
            if success and result.stdout.strip():
                return int(result.stdout.strip())
        except:
            pass
        return 44100

    def _get_peak_amplitude(self, file_path):
        try:
            cmd = ['ffmpeg', '-i', file_path, 
                '-af', 'volumedetect', 
                '-f', 'null', '-']
            success, result = self._safe_subprocess_run(cmd, "peak detect", allow_fail=True)
            if success and result and result.stderr:
                match = re.search(r'max_volume:\s*([-\d.]+)\s*dB', result.stderr)
                if match:
                    db = float(match.group(1))
                    return 10 ** (db / 20)
        except:
            pass
        return 0.95

    def _build_filters(self, current_input=None):
        filters = []

        if self.settings['methods'].get('spectral_masking', False):
            if current_input and os.path.exists(current_input):
                sensitivity = self.settings.get('spectral_mask_sensitivity', 0.8)
                attenuation = self.settings.get('spectral_mask_attenuation', 12)
                num_peaks = self.settings.get('spectral_mask_peaks', 10)
                
                mask_filter = self._build_spectral_mask_filter(
                    current_input, 
                    num_peaks=num_peaks,
                    sensitivity=sensitivity, 
                    attenuation=attenuation
                )
                if mask_filter:
                    filters.append(mask_filter)
        
        if self.settings['methods'].get('concert_emulation', False):
            intensity = self.settings.get('concert_intensity', 'medium')
            concert_filter = self._build_concert_emulation_filter(intensity)
            if concert_filter:
                filters.append(concert_filter)
        
        if self.settings['methods'].get('midside_processing', False):
            mid_gain = self.settings.get('midside_mid_gain', -3)
            side_gain = self.settings.get('midside_side_gain', 2)
            ms_filter = self._build_midside_filter(mid_eq_gain=mid_gain, side_eq_gain=side_gain)
            if ms_filter:
                filters.append(ms_filter)
        
        if self.settings['methods'].get('psychoacoustic_noise', False):
            intensity = self.settings.get('psychoacoustic_intensity', 0.0003)
            noise_filter = self._build_psychoacoustic_noise_filter(intensity)
            if noise_filter:
                filters.append(noise_filter)
        
        if self.settings['methods'].get('saturation', False):
            drive = self.settings.get('saturation_drive', 1.5)
            mix = self.settings.get('saturation_mix', 0.15)
            sat_filter = self._build_saturation_filter(drive, mix)
            if sat_filter:
                filters.append(sat_filter)
        
        if self.settings['methods'].get('temporal_jitter', False):
            intensity = self.settings.get('jitter_intensity', 0.002)
            freq = self.settings.get('jitter_frequency', 0.5)
            jitter_filter = self._build_temporal_jitter_filter(intensity, freq)
            if jitter_filter:
                filters.append(jitter_filter)
        
        if self.settings['methods'].get('spectral_jitter', False):
            num_notches = self.settings.get('spectral_jitter_count', 5)
            att = self.settings.get('spectral_jitter_attenuation', 15)
            fixed_freqs = self.settings.get('spectral_jitter_fixed_frequencies', None)
            fixed_att = self.settings.get('spectral_jitter_fixed_attenuation', None)
            manual_cfg = self.settings.get('spectral_jitter_manual_config', None)
            spec_jitter = self._build_spectral_jitter_filter(num_notches, att, fixed_freqs, fixed_att, manual_cfg)
            if spec_jitter:
                filters.append(spec_jitter)
        
        if self.settings['methods'].get('loudnorm', False):
            target = self.settings.get('loudnorm_target', -14.0)
            filters.append(f"loudnorm=I={target:.1f}:TP=-1.5:LRA=11")

        if self.settings['methods'].get('resample_drift', False):
            drift = self.settings.get('resample_drift_amount', 1)
            sr = self._get_sample_rate(current_input) if current_input else 44100
            filters.append(f"asetrate={sr + drift},aresample={sr}:resampler=soxr:precision=28")
            
        if self.settings['methods'].get('dc_shift', False):
            filters.append(f"dcshift={self.settings.get('dc_shift_value', 0.000005)}")
            
        if self.settings['methods'].get('phase_invert', False):
            strength = self.settings.get('phase_invert_strength', 1.0)
            filters.append(f"pan=stereo|c0=c0|c1={-strength}*c1")
            
        if self.settings['methods'].get('phase_scramble', False):
            speed = self.settings.get('phase_scramble_speed', 2.0)
            filters.append(f"aphaser=type=t:delay=0.1:decay=0:speed={speed}")
            
        if self.settings['methods'].get('haas_delay', False):
            delay_ms = self.settings.get('haas_delay_ms', 15.0)
            filters.append(f"adelay=0|{int(round(delay_ms))}")
            
        if self.settings['methods'].get('pitch', False):
            semitones = self.settings['pitch_value']
            sr = self._get_sample_rate(current_input) if current_input else 44100
            rate = sr * (2 ** (semitones / 12))
            filters.append(f"asetrate={rate:.0f},aresample={sr}")
            
        if self.settings['methods'].get('speed', False):
            filters.append(f"atempo={self.settings['speed_value']}")
            
        if self.settings['methods'].get('eq', False):
            if self.settings.get('eq_type') == 1:
                filters.append("equalizer=f=1000:width_type=o:width=2:g=-4")
                filters.append("equalizer=f=2000:width_type=o:width=2:g=-2")
            elif self.settings.get('eq_type') == 2:
                filters.append("equalizer=f=8000:width_type=o:width=2:g=3")
            else:
                filters.append(f"equalizer=f=1000:width_type=o:width=2:g={-self.settings['eq_value']}")
                
        if self.settings['methods'].get('silence', False):
            filters.append(f"apad=pad_dur={self.settings['silence_duration']}")
            
        return ", ".join(filters) if filters else None

    def _temp_ext(self):
        return '.wav' if self.settings.get('lossless_intermediate', False) else '.mp3'

    def _temp_codec(self):
        if self.settings.get('lossless_intermediate', False):
            return ['-c:a', 'pcm_s16le']
        return ['-c:a', 'libmp3lame', '-q:a', '2']

    def _verify_audio(self, file_path):
        if file_path.endswith('.wav'):
            return os.path.exists(file_path) and os.path.getsize(file_path) > 0
        return self._verify_mp3(file_path)

    def run(self):
        success_count = 0
        total = len(self.files)

        for i, (file_path, track_info) in enumerate(zip(self.files, self.tracks_info)):
            if self._stop_event.is_set():
                break
            temp_files = []
            current_input = file_path

            try:
                self.on_progress(i + 1, total, file_path)

                if not self._verify_mp3(current_input):
                    self.on_file_complete(file_path, False, "")
                    self.on_error(f"Исходный файл повреждён: {os.path.basename(file_path)}")
                    continue

                original_name = os.path.splitext(os.path.basename(file_path))[0]
                tpl = self.settings.get('filename_template', 'VK_{n:03d}_custom')
                ex_title  = self.metadata.get('title')  or (track_info.title if track_info else original_name)
                ex_artist = self.metadata.get('artist') or (track_info.artist if track_info else '')
                ex_album  = self.metadata.get('album')  or (track_info.album if track_info else '')
                ex_year   = self.metadata.get('year')   or (track_info.year if track_info else '')
                try:
                    output_filename = tpl.format(
                        n=self.start_index + i + 1,
                        original=self._safe_filename(original_name),
                        title=self._safe_filename(ex_title),
                        artist=self._safe_filename(ex_artist),
                        album=self._safe_filename(ex_album),
                        year=self._safe_filename(str(ex_year)),
                    ) + '.mp3'
                    output_filename = re.sub(r'[\\/*?:"<>|]', '_', output_filename)
                except (KeyError, ValueError, IndexError):
                    output_filename = f"VK_{i+1:03d}_custom.mp3"

                output_file = os.path.join(self.output_dir, output_filename)

                if self.settings['methods'].get('cut_fragment', False):
                    cut_pos_percent = self.settings.get('cut_position_percent', 50)
                    cut_dur = self.settings.get('cut_duration', 2)
                    duration = self._get_duration(current_input)
                    if duration > 0:
                        cut_start = max(0, (duration * cut_pos_percent / 100) - (cut_dur / 2))
                        cut_end = min(duration, cut_start + cut_dur)

                        part1 = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
                        part1.close()
                        temp_files.append(part1.name)
                        part2 = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
                        part2.close()
                        temp_files.append(part2.name)

                        s1, _ = self._safe_subprocess_run(
                            ['ffmpeg', '-i', current_input, '-t', str(cut_start)]
                            + self._temp_codec() + ['-y', part1.name], "cut part 1")
                        s2, _ = self._safe_subprocess_run(
                            ['ffmpeg', '-i', current_input, '-ss', str(cut_end)]
                            + self._temp_codec() + ['-y', part2.name], "cut part 2")

                        if s1 and s2:
                            concat_list = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
                            concat_list.write(f"file '{part1.name}'\nfile '{part2.name}'")
                            concat_list.close()
                            temp_files.append(concat_list.name)
                            cut_result = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
                            cut_result.close()
                            temp_files.append(cut_result.name)
                            sc, _ = self._safe_subprocess_run(
                                ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', concat_list.name]
                                + self._temp_codec() + ['-y', cut_result.name], "concat parts")
                            if sc and self._verify_audio(cut_result.name):
                                current_input = cut_result.name

                if self.settings['methods'].get('trim_silence', False):
                    trim_dur = self.settings.get('trim_duration', 5)
                    threshold = self.settings.get('trim_silence_threshold', -60)
                    silence_end = 0.0
                    detect_cmd = ['ffmpeg', '-i', current_input, '-af',
                                  f'silencedetect=noise={threshold}dB:duration=0.3',
                                  '-f', 'null', '-']
                    ok, res = self._safe_subprocess_run(detect_cmd, "silence detect", allow_fail=True)
                    if ok and res and res.stderr:
                        match = re.search(r'silence_end:\s*([\d.]+)', res.stderr)
                        if match:
                            silence_end = float(match.group(1))
                    if silence_end > 0:
                        silence_end = min(silence_end, trim_dur)
                        trim_temp = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
                        trim_temp.close()
                        temp_files.append(trim_temp.name)
                        success, _ = self._safe_subprocess_run(
                            ['ffmpeg', '-i', current_input, '-ss', str(silence_end)]
                            + self._temp_codec() + ['-y', trim_temp.name], "trim silence")
                        if success and self._verify_audio(trim_temp.name):
                            current_input = trim_temp.name

                if self.settings['methods'].get('merge', False) and self.settings.get('extra_track_path'):
                    extra_track = self.settings['extra_track_path']
                    if os.path.exists(extra_track) and self._verify_mp3(extra_track):
                        norm1 = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
                        norm1.close()
                        temp_files.append(norm1.name)
                        norm2 = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
                        norm2.close()
                        temp_files.append(norm2.name)

                        s1, r1 = self._safe_subprocess_run(
                            ['ffmpeg', '-i', current_input, '-ar', '44100', '-ac', '2',
                             '-c:a', 'pcm_s16le', '-y', norm1.name], "normalize main")
                        s2, r2 = self._safe_subprocess_run(
                            ['ffmpeg', '-i', extra_track, '-ar', '44100', '-ac', '2',
                             '-c:a', 'pcm_s16le', '-y', norm2.name], "normalize extra")

                        if s1 and s2 and os.path.getsize(norm1.name) > 0 and os.path.getsize(norm2.name) > 0:
                            concat_list = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8')
                            concat_list.write(f"file '{norm1.name.replace(chr(92), '/')}'\n")
                            concat_list.write(f"file '{norm2.name.replace(chr(92), '/')}'\n")
                            concat_list.close()
                            temp_files.append(concat_list.name)
                            merge_temp = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
                            merge_temp.close()
                            temp_files.append(merge_temp.name)
                            success, result = self._safe_subprocess_run(
                                ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', concat_list.name,
                                 '-codec:a', 'libmp3lame', '-q:a', '2', '-y', merge_temp.name], "concat wav")
                            if success and self._verify_mp3(merge_temp.name):
                                current_input = merge_temp.name
                            else:
                                self._try_alternative_merge(current_input, extra_track, temp_files, merge_temp.name)
                                if os.path.exists(merge_temp.name) and self._verify_mp3(merge_temp.name):
                                    current_input = merge_temp.name

                if self.settings['methods'].get('vk_infrasonic', False):
                    duration = self._get_duration(current_input)
                    if duration and duration > 0:
                        sample_rate = self._get_sample_rate(current_input) or 44100

                        amplitude = self.settings.get('vk_infrasonic_amplitude', 0.35)
                        if self.settings.get('vk_infrasonic_adaptive_amplitude', True):
                            peak = self._get_peak_amplitude(current_input)
                            if peak > 0:
                                headroom_factor = max(0.3, (0.98 - peak) / 0.98)
                                amplitude = amplitude * headroom_factor

                        local_vk = {**self.settings, 'vk_infrasonic_amplitude': amplitude}
                        expr_l = self._get_vk_infrasonic_expr(local_vk, extra_phase=0.0)
                        expr_r = self._get_vk_infrasonic_expr(local_vk, extra_phase=0.5236)

                        vk_temp = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
                        vk_temp.close()
                        temp_files.append(vk_temp.name)

                        fc = (
                            f"[0:a]volume=0.97[main];"
                            f"aevalsrc='{expr_l}|{expr_r}':c=stereo:s={sample_rate}[infra];"
                            f"[main][infra]amix=inputs=2:duration=first:"
                            f"dropout_transition=0:normalize=0[out]"
                        )
                        
                        cmd_vk = [
                            'ffmpeg', '-i', current_input,
                            '-filter_complex', fc,
                            '-map', '[out]',
                            '-codec:a', 'libmp3lame', '-q:a', '2',
                            '-y', vk_temp.name
                        ]
                        
                        success_vk, result_vk = self._safe_subprocess_run(cmd_vk, "vk infrasonic", allow_fail=False)
                        if success_vk and self._verify_mp3(vk_temp.name):
                            current_input = vk_temp.name
                        else:
                            err = self._extract_ffmpeg_error(result_vk.stderr) if result_vk else "неизвестная ошибка"
                            self.on_error(f"VK Инфразвук: не удалось применить — {err}")

                ultrasonic_temp = None
                if self.settings['methods'].get('ultrasonic_noise', False):
                    ultrasonic_temp = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
                    ultrasonic_temp.close()
                    temp_files.append(ultrasonic_temp.name)
                    freq = self.settings.get('ultrasonic_freq', 21000)
                    level = self.settings.get('ultrasonic_level', 0.001)
                    duration = self._get_duration(current_input)
                    if duration > 0:
                        ok_ultra, _ = self._safe_subprocess_run(
                            ['ffmpeg', '-f', 'lavfi',
                             '-i', f'sine=frequency={freq}:sample_rate=44100:duration={duration}',
                             '-af', f'volume={level}', '-y', ultrasonic_temp.name],
                            "ultrasonic", allow_fail=True)
                        if not ok_ultra:
                            self.on_error(f"Ультразвук: не удалось сгенерировать сигнал {freq} Гц")

                filters = self._build_filters(current_input)

                if self.settings['methods'].get('fade_out', False):
                    fade_dur = self.settings.get('fade_duration', 5)
                    duration = self._get_duration(current_input)
                    if duration > 0:
                        fade_start = max(0, duration - fade_dur)
                        fade_f = f"afade=t=out:st={fade_start}:d={fade_dur}"
                        filters = f"{filters},{fade_f}" if filters else fade_f

                cover_source_path = None
                if self.settings.get('selected_cover_path') and os.path.exists(self.settings['selected_cover_path']):
                    cover_source_path = self.settings['selected_cover_path']
                elif self.settings.get('preserve_cover') and track_info and track_info.cover_data:
                    cover_ext = track_info.cover_mime.split('/')[1] if '/' in track_info.cover_mime else 'jpg'
                    cover_temp = tempfile.NamedTemporaryFile(suffix=f'.{cover_ext}', delete=False)
                    cover_temp.write(track_info.cover_data)
                    cover_temp.close()
                    temp_files.append(cover_temp.name)
                    cover_source_path = cover_temp.name

                has_ultrasonic = (
                    ultrasonic_temp and
                    os.path.exists(ultrasonic_temp.name) and
                    os.path.getsize(ultrasonic_temp.name) > 0
                )

                cmd = ['ffmpeg', '-i', current_input]

                if has_ultrasonic:
                    cmd.extend(['-i', ultrasonic_temp.name])
                    cover_idx = 2
                    amix = "[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0:normalize=0"
                    if filters:
                        fc = f"{amix}[_mixed];[_mixed]{filters}[_out]"
                    else:
                        fc = f"{amix}[_out]"
                    if cover_source_path:
                        cmd.extend(['-i', cover_source_path])
                        cmd.extend(['-filter_complex', fc, '-map', '[_out]', '-map', f'{cover_idx}:v'])
                        if cover_source_path.lower().endswith(('.jpg', '.jpeg')):
                            cmd.extend(['-c:v', 'copy', '-disposition:v', 'attached_pic'])
                        else:
                            cmd.extend(['-c:v', 'mjpeg', '-q:v', '2', '-disposition:v', 'attached_pic'])
                    else:
                        cmd.extend(['-filter_complex', fc, '-map', '[_out]'])
                else:
                    if cover_source_path:
                        cmd.extend(['-i', cover_source_path])
                        cmd.extend(['-map', '0:a', '-map', '1:v'])
                        if cover_source_path.lower().endswith(('.jpg', '.jpeg')):
                            cmd.extend(['-c:v', 'copy', '-disposition:v', 'attached_pic'])
                        else:
                            cmd.extend(['-c:v', 'mjpeg', '-q:v', '2', '-disposition:v', 'attached_pic'])
                    else:
                        cmd.extend(['-map', '0:a'])
                    if filters:
                        cmd.extend(['-af', filters])

                if self.settings.get('rename_files', True):
                    titles = ["Track", "Song", "Melody", "Rhythm", "Harmony", "Beat", "Flow", "Vibe", "Sound", "Wave"]
                    title_to_use = f"{random.choice(titles)} {random.randint(1, 999)}"
                else:
                    title_to_use = self.metadata.get('title') or (track_info.title if track_info and self.settings.get('preserve_metadata') else "")

                if title_to_use:
                    cmd.extend(['-metadata', f'title={title_to_use}'])

                artist = self.metadata.get('artist') or (track_info.artist if track_info and self.settings.get('preserve_metadata') else "")
                if artist:
                    cmd.extend(['-metadata', f'artist={artist}'])
                album = self.metadata.get('album') or (track_info.album if track_info and self.settings.get('preserve_metadata') else "")
                if album:
                    cmd.extend(['-metadata', f'album={album}'])
                year = self.metadata.get('year') or (track_info.year if track_info and self.settings.get('preserve_metadata') else "")
                if year:
                    cmd.extend(['-metadata', f'date={year}'])
                genre = self.metadata.get('genre') or (track_info.genre if track_info and self.settings.get('preserve_metadata') else "")
                if genre:
                    cmd.extend(['-metadata', f'genre={genre}'])

                if self.settings['methods'].get('fake_metadata', False):
                    fake_text = ''.join(random.choices(
                        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                        k=random.randint(100, 500)))
                    cmd.extend(['-metadata', f'comment={fake_text}'])

                if self.settings['methods'].get('bitrate_jitter', False):
                    bitrate = random.choice([192, 224, 256, 320])
                    cmd.extend(['-codec:a', 'libmp3lame', '-b:a', f'{bitrate}k'])
                else:
                    quality = self.settings.get('quality', '0')
                    if str(quality).endswith('k'):
                        cmd.extend(['-codec:a', 'libmp3lame', '-b:a', str(quality)])
                    else:
                        cmd.extend(['-codec:a', 'libmp3lame', '-q:a', str(quality)])

                if self.settings['methods'].get('dither_attack', False):
                    dither = self.settings.get('dither_method', 'triangular_hp')
                    cmd.extend(['-dither_method', dither])

                if self.settings['methods'].get('frame_shift', False):
                    cmd.extend(['-write_xing', '0'])

                cmd.extend(['-id3v2_version', '3', '-y', output_file])

                success, result = self._safe_subprocess_run(cmd, "main processing")

                if success and os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    if self.settings['methods'].get('id3_padding_attack', False):
                        try:
                            self._apply_id3_padding_attack(output_file, self.settings.get('id3_padding_bytes', 512))
                        except Exception as e:
                            self.on_error(f"ID3 Padding Attack: {e}")
                    if self.settings['methods'].get('reorder_tags', False):
                        try:
                            self._reorder_id3_tags(output_file)
                        except Exception as e:
                            self.on_error(f"Reorder Tags: {e}")
                    if self.settings['methods'].get('broken_duration', False):
                        try:
                            self._apply_broken_duration(output_file, self.settings.get('broken_type', 0))
                        except Exception as e:
                            self.on_error(f"Broken Duration: {e}")

                for temp_file in temp_files:
                    try:
                        if os.path.exists(temp_file):
                            os.unlink(temp_file)
                    except:
                        pass

                if success and os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    success_count += 1
                    self.on_file_complete(file_path, True, output_file)
                    if self.settings.get('delete_original', False):
                        try:
                            os.unlink(file_path)
                        except:
                            pass
                else:
                    self.on_file_complete(file_path, False, "")
                    if result:
                        self.on_error(f"FFmpeg: {self._extract_ffmpeg_error(result.stderr)}")

            except Exception as e:
                for temp_file in temp_files:
                    try:
                        if os.path.exists(temp_file):
                            os.unlink(temp_file)
                    except:
                        pass
                self.on_file_complete(file_path, False, "")
                self.on_error(f"Ошибка: {str(e)}")

        self.on_all_complete(success_count, total)

    def _apply_id3_padding_attack(self, file_path, padding_bytes):
        audio = MP3(file_path)
        if audio.tags is None:
            audio.add_tags()
        safe_pad_size = min(padding_bytes, 2048)
        garbage_text = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=safe_pad_size))
        audio.tags['TXXX:VK_PAD'] = TXXX(encoding=3, desc='VK_Modifier_Padding', text=garbage_text)
        audio.tags['TXXX:VK_META_NOISE'] = TXXX(
            encoding=3, desc='meta_noise',
            text=random.getrandbits(128).to_bytes(16, 'big').hex())
        audio.save(v2_version=3, v23_sep='/', padding=lambda info: 1024)

    def _reorder_id3_tags(self, file_path):
        audio = MP3(file_path)
        if audio.tags:
            audio.tags.update_to_v23()
            audio.save()

    def _apply_broken_duration(self, file_path, bug_type):
        audio = MP3(file_path)
        if audio.tags is None:
            audio.add_tags()
        if bug_type == 0:
            fake_ms = random.randint(3_600_000, 36_000_000)
        elif bug_type == 1:
            fake_ms = random.randint(100, 3_000)
        elif bug_type == 2:
            fake_ms = random.randint(60_000, 7_200_000)
        else:
            fake_ms = 16_777_215 * 1000
        audio.tags['TLEN'] = TLEN(encoding=3, text=str(fake_ms))
        audio.save(v2_version=3)

        with open(file_path, 'rb') as f:
            data = bytearray(f.read())

        vbr_pos = data.find(b'Xing')
        if vbr_pos == -1:
            vbr_pos = data.find(b'Info')

        if 0 < vbr_pos < len(data) - 120:
            flags = int.from_bytes(data[vbr_pos + 4: vbr_pos + 8], 'big')

            if flags & 0x01:
                frame_offset = vbr_pos + 8
                real_frames = int.from_bytes(data[frame_offset: frame_offset + 4], 'big')

                if bug_type == 0:
                    mult = random.randint(50, 200)
                    fake_frames = min(real_frames * mult if real_frames > 0 else 0x00500000,
                                      0xFFFFFF00)
                elif bug_type == 1:
                    fake_frames = random.randint(1, 50)
                elif bug_type == 2:
                    fake_frames = random.randint(0x00100000, 0x00EFFFFF)
                else:
                    fake_frames = 0xFFFFFF00

                data[frame_offset: frame_offset + 4] = fake_frames.to_bytes(4, 'big')

                if flags & 0x02:
                    byte_offset = frame_offset + 4
                    fake_bytes = random.randint(0x01000000, 0x7FFFFFFF)
                    data[byte_offset: byte_offset + 4] = fake_bytes.to_bytes(4, 'big')

                with open(file_path, 'wb') as f:
                    f.write(data)


