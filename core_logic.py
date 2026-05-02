import hashlib
import os
import random
import subprocess
import tempfile
import threading
import re
import json
from datetime import timedelta
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, TCON, APIC, TLEN, TXXX, PRIV


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
        except:
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
        except:
            pass


class ModificationWorker(threading.Thread):
    def __init__(self, files, tracks_info, output_dir, settings, metadata,
                 on_progress=None, on_file_complete=None, on_all_complete=None, on_error=None,
                 start_index=0):
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

    @staticmethod
    def _safe_filename(s):
        """Убирает символы, недопустимые в именах файлов Windows."""
        import re as _re
        return _re.sub(r'[\\/*?:"<>|]', '_', str(s)).strip() or '_'

    @staticmethod
    def _extract_ffmpeg_error(stderr, chars=800):
        """Пропускает шапку ffmpeg (версия/конфигурация) и возвращает реальную ошибку."""
        lines = stderr.splitlines()
        error_lines = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            # Пропускаем строки шапки
            if any(stripped.startswith(p) for p in (
                'ffmpeg version', 'built with', 'configuration:', 'lib', 'Copyright'
            )):
                continue
            error_lines.append(line)
        result_text = '\n'.join(error_lines)
        return result_text[-chars:] if len(result_text) > chars else result_text

    def _safe_subprocess_run(self, cmd, description="", allow_fail=False):
        try:
            result = subprocess.run(cmd, capture_output=True, encoding='utf-8', errors='ignore', timeout=300)
            if result.returncode != 0 and not allow_fail:
                err = self._extract_ffmpeg_error(result.stderr)
                print(f"FFmpeg error in {description}:\n{err}")
                return False, result
            return True, result
        except subprocess.TimeoutExpired:
            print(f"Timeout in {description}")
            return False, None
        except Exception as e:
            print(f"Exception in {description}: {e}")
            return False, None

    def _try_alternative_merge(self, main_track, extra_track, temp_files, output_path):
        try:
            cmd = [
                'ffmpeg', '-i', main_track, '-i', extra_track,
                '-filter_complex', '[0:a][1:a]concat=n=2:v=0:a=1[out]',
                '-map', '[out]', '-codec:a', 'libmp3lame', '-q:a', '2',
                '-y', output_path
            ]
            success, result = self._safe_subprocess_run(cmd, "alternative merge")
            if not success:
                print(f"Alternative merge failed: {result.stderr if result else 'unknown'}")
        except Exception as e:
            print(f"Alternative merge exception: {e}")

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
        """Анализирует спектр аудио и строит цепочку notch-фильтров для подавления наиболее громких частотных пиков.

        Returns comma-joined ffmpeg equalizer filter string, or None if analysis fails.
        """
        bands = [
            40, 50, 63, 80, 100, 125, 160, 200, 250, 315, 400, 500, 630,
            800, 1000, 1250, 1600, 2000, 2500, 3150, 4000, 5000, 6300,
            8000, 10000, 12500, 16000
        ]
        
        band_energies = []

        for center_freq in bands:
            # Используем bandpass фильтр и измеряем RMS энергию
            cmd = [
                'ffmpeg', '-i', audio_path,
                '-af', f'bandpass=f={center_freq}:width={center_freq//4}:csg=1, astats=measure=peak:measure=RMS',
                '-f', 'null', '-'
            ]
            success, result = self._safe_subprocess_run(cmd, f"analyzing {center_freq}Hz", allow_fail=True)
            
            if success and result and result.stderr:
                # Парсим RMS из вывода astats
                rms_match = re.search(r'RMS level dB:\s*([-\d.]+)', result.stderr)
                if not rms_match:
                    rms_match = re.search(r'RMS level:\s*([-\d.]+)', result.stderr)
                
                if rms_match:
                    rms_db = float(rms_match.group(1))
                    if rms_db > -80:
                        band_energies.append((center_freq, rms_db))
        
        if not band_energies:
            print("Spectral Masking: не удалось измерить энергию полос")
            return None

        energies = [e for _, e in band_energies]
        avg_energy = sum(energies) / len(energies)
        variance = sum((e - avg_energy) ** 2 for e in energies) / len(energies)
        std_dev = variance ** 0.5
        threshold = avg_energy + (sensitivity * std_dev)

        peaks = [(freq, e) for freq, e in band_energies if e > threshold]
        peaks.sort(key=lambda x: x[1], reverse=True)
        peaks = peaks[:num_peaks]

        if not peaks:
            print("Spectral Masking: значимых пиков не найдено")
            return None

        eq_filters = []
        for freq, energy in peaks:
            eq_filters.append(f"equalizer=f={freq}:width_type=o:width=0.15:g=-{attenuation}")
            print(f"  -> Подавляем {freq} Hz (энергия: {energy:.1f} dB)")
        
        print(f"Spectral Masking: подавлено {len(eq_filters)} спектральных пиков")
        return ", ".join(eq_filters)

    def _build_spectral_mask_filter_v2(self, audio_path, num_peaks=10, attenuation=10):
        """Альтернативный анализ спектра через volumedetect по фиксированному набору полос.

        Менее точен, чем v1, но быстрее. Returns comma-joined filter string or None.
        """
        try:
            bands = [100, 200, 400, 800, 1000, 2000, 3000, 4000, 6000, 8000, 10000, 12000, 15000]
            peaks = []

            for freq in bands:
                cmd = [
                    'ffmpeg', '-i', audio_path,
                    '-af', f'bandpass=f={freq}:width=100, volumedetect',
                    '-f', 'null', '-'
                ]
                success, result = self._safe_subprocess_run(cmd, f"detect {freq}Hz", allow_fail=True)
                if success and result and result.stderr:
                    # Ищем mean_volume
                    vol_match = re.search(r'mean_volume:\s*([-\d.]+)', result.stderr)
                    if vol_match:
                        mean_vol = float(vol_match.group(1))
                        if mean_vol > -30:
                            peaks.append(freq)
            
            if peaks:
                eq_filters = []
                for freq in peaks[:num_peaks]:
                    eq_filters.append(f"equalizer=f={freq}:width_type=o:width=0.2:g=-{attenuation}")
                return ", ".join(eq_filters)
                
        except Exception as e:
            print(f"Spectral mask v2 failed: {e}")
        
        return None

    def _build_concert_emulation_filter(self, intensity='medium'):
        """Строит ffmpeg-фильтр эмуляции концертной записи: сужение стереобазы, реверберация, компрессия, EQ.

        intensity: 'light' | 'medium' | 'heavy'
        """
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
            f"compand=attacks=0.1:decays=0.3:points=-80/-80|-30/-20|0/-3:gain={cfg['compressor']}"
        )

        if cfg['eq_low'] != 0:
            filters.append(f"bass=g={cfg['eq_low']}:f=100:width_type=q:width=0.7")
        if cfg['eq_high'] != 0:
            filters.append(f"treble=g={cfg['eq_high']}:f=8000:width_type=q:width=0.7")

        return ", ".join(filters)
    
    def _build_midside_filter(self, mid_eq_gain=-3, side_eq_gain=2, mid_freq=1000, side_freq=8000):
        """Строит pan-фильтр для раздельного усиления Mid (центр) и Side (боки) в dB.

        Реализует M/S матрицу без filter_complex через одну команду pan=stereo.
        """
        import math
        mid_lin  = 10 ** (mid_eq_gain  / 20)
        side_lin = 10 ** (side_eq_gain / 20)
        a = (mid_lin + side_lin) / 2
        b = (mid_lin - side_lin) / 2
        return f"pan=stereo|c0={a:.4f}*c0+{b:.4f}*c1|c1={b:.4f}*c0+{a:.4f}*c1"

    def _build_psychoacoustic_noise_filter(self, intensity=0.0003):
        """Строит flanger-фильтр с малой глубиной для создания высокочастотных интерференционных паттернов."""
        depth = min(int(intensity * 500000), 10)
        depth = max(1, depth)
        return f"flanger=delay=0:depth={depth}:speed=0.3:shape=sinusoidal:phase=0:interp=linear"

    def _build_saturation_filter(self, drive=1.5, mix=0.15):
        """Строит цепочку aemphasis + acrusher, имитирующих аналоговое ленточное насыщение."""
        mix_clamped = max(0.0, min(1.0, mix))
        drive_clamped = max(1.0, min(5.0, drive))
        return (
            f"aemphasis=level_in=1:level_out=1:mode=reproduction,"
            f"acrusher=level_in={drive_clamped:.2f}:level_out=1:bits=12:mode=log:mix={mix_clamped:.2f}"
        )

    def _build_temporal_jitter_filter(self, intensity=0.002, frequency=0.5):
        """Строит vibrato-фильтр, создающий синусоидальную модуляцию высоты тона (эмуляция нестабильности носителя)."""
        freq_clamped = max(0.1, min(20.0, frequency))
        depth = max(0.0, min(1.0, intensity * 100))  # 0.002 → 0.2
        return f"vibrato=f={freq_clamped:.2f}:d={depth:.3f}"

    def _build_spectral_jitter_filter(self, num_notches=5, max_attenuation=15):
        """Строит цепочку псевдослучайных notch-фильтров в слышимом диапазоне."""
        filters = []
        freq_pool = [
            120, 250, 400, 630, 800, 1200, 1600, 2000, 
            2500, 3150, 4000, 5000, 6300, 8000, 10000, 12500
        ]
        
        selected = random.sample(freq_pool, min(num_notches, len(freq_pool)))
        
        for freq in selected:
            att = random.randint(max_attenuation // 2, max_attenuation)
            width = random.uniform(0.1, 0.3)
            filters.append(f"equalizer=f={freq}:width_type=o:width={width:.2f}:g=-{att}")
        
        return ", ".join(filters)

    def _get_vk_infrasonic_expr(self, settings):
        """Генерирует aevalsrc-выражение для инфразвуковой синусоиды.

        Режимы (vk_infrasonic_mode): simple, modulated, phase, harmonic, maximum, custom.
        При vk_infrasonic_random_phase=True каждый вызов даёт уникальную фазу.
        """
        
        freq = settings.get('vk_infrasonic_freq', 18.0)
        amp = settings.get('vk_infrasonic_amplitude', 0.35)
        mode = settings.get('vk_infrasonic_mode', 'modulated')
        mod_freq = settings.get('vk_infrasonic_mod_freq', 0.08)
        mod_depth = settings.get('vk_infrasonic_mod_depth', 0.3)
        phase_shift = settings.get('vk_infrasonic_phase_shift', 0.0)
        harmonics = settings.get('vk_infrasonic_harmonics', [0.15, 0.07, 0.03])
        waveform = settings.get('vk_infrasonic_waveform', 'sine')
        custom_expr = settings.get('vk_infrasonic_custom_expr', '')
        
        if settings.get('vk_infrasonic_random_phase', True):
            import random
            phase_shift = random.uniform(0, 2 * 3.141592653589793)
        
        def wave_func(wave_type, arg):
            if wave_type == 'sine':
                return f"sin({arg})"
            elif wave_type == 'triangle':
                return f"2/3.141592653589793*asin(sin({arg}))"
            elif wave_type == 'square':
                return f"sin({arg})/(abs(sin({arg}))+0.000001)"
            else:
                return f"sin({arg})"
        
        if mode == 'custom' and custom_expr:
            return custom_expr

        base_arg = f"2*3.141592653589793*{freq}*t"
        if phase_shift != 0:
            base_arg = f"({base_arg}+{phase_shift})"
        
        if mode == 'simple':
            expr = f"{amp}*{wave_func(waveform, base_arg)}"
            
        elif mode == 'modulated':
            mod_term = f"(1-{mod_depth}+{mod_depth}*sin(2*3.141592653589793*{mod_freq}*t))"
            expr = f"{amp}*{mod_term}*{wave_func(waveform, base_arg)}"
            
        elif mode == 'phase':
            phase_mod = f"{mod_depth}*sin(2*3.141592653589793*{mod_freq}*t)"
            mod_arg = f"({base_arg}+{phase_mod})"
            expr = f"{amp}*{wave_func(waveform, mod_arg)}"
            
        elif mode == 'harmonic':
            terms = [f"{amp}*{wave_func(waveform, base_arg)}"]
            for i, h_amp in enumerate(harmonics, start=2):
                if h_amp > 0:
                    h_freq = freq * i
                    h_arg = f"2*3.141592653589793*{h_freq}*t"
                    if phase_shift != 0:
                        h_arg = f"({h_arg}+{phase_shift})"
                    terms.append(f"{amp * h_amp}*{wave_func(waveform, h_arg)}")
            expr = "+".join(terms)
            
        else:  # maximum
            mod_term = f"(1-{mod_depth}+{mod_depth}*sin(2*3.141592653589793*{mod_freq}*t))"
            main_term = f"{amp}*{mod_term}*{wave_func(waveform, base_arg)}"
            
            if harmonics and len(harmonics) > 0 and harmonics[0] > 0:
                h2_amp = harmonics[0]
                h2_arg = f"2*3.141592653589793*{freq*2}*t"
                if phase_shift != 0:
                    h2_arg = f"({h2_arg}+{phase_shift})"
                harm_term = f"{amp * h2_amp}*{mod_term}*{wave_func(waveform, h2_arg)}"
                expr = f"{main_term}+{harm_term}"
            else:
                expr = main_term
        
        return expr

    def _get_sample_rate(self, file_path):
        """Определяет частоту дискретизации аудиофайла"""
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
        """Определяет пиковую амплитуду аудио в линейном масштабе (0.0 - 1.0)"""
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
    
    def _get_dynamic_range(self, file_path):
        """Вычисляет динамический диапазон трека (разница Peak - RMS в dB)"""
        try:
            cmd = ['ffmpeg', '-i', file_path,
                '-af', 'astats=measure=peak:measure=RMS',
                '-f', 'null', '-']
            success, result = self._safe_subprocess_run(cmd, "dynamic range", allow_fail=True)
            if success and result and result.stderr:
                peak_match = re.search(r'Peak level dB:\s*([-\d.]+)', result.stderr)
                rms_match = re.search(r'RMS level dB:\s*([-\d.]+)', result.stderr)
                if peak_match and rms_match:
                    peak_db = float(peak_match.group(1))
                    rms_db = float(rms_match.group(1))
                    return abs(peak_db - rms_db)
        except:
            pass
        return 6.0

    def _build_filters(self, current_input=None):
        """Собирает ffmpeg -af фильтр-цепочку из всех включённых методов.

        current_input передаётся для предварительного анализа спектра (spectral_masking).
        Returns comma-joined filter string or None if no filters are enabled.
        """
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
            spec_jitter = self._build_spectral_jitter_filter(num_notches, att)
            if spec_jitter:
                filters.append(spec_jitter)
        
        if self.settings['methods'].get('resample_drift', False):
            drift = self.settings.get('resample_drift_amount', 1)
            filters.append(f"asetrate={44100 + drift},aresample=44100:resampler=soxr:precision=28")
            
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
            filters.append(f"adelay=0|{delay_ms}")
            
        if self.settings['methods'].get('pitch', False):
            semitones = self.settings['pitch_value']
            rate = 44100 * (2 ** (semitones / 12))
            filters.append(f"asetrate={rate:.0f},aresample=44100")
            
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

    def run(self):
        success_count = 0
        total = len(self.files)

        for i, (file_path, track_info) in enumerate(zip(self.files, self.tracks_info)):
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
                ex_title  = self.metadata.get('title')  or track_info.title  or original_name
                ex_artist = self.metadata.get('artist') or track_info.artist or ''
                ex_album  = self.metadata.get('album')  or track_info.album  or ''
                ex_year   = self.metadata.get('year')   or track_info.year   or ''
                try:
                    output_filename = tpl.format(
                        n=self.start_index + i + 1,
                        original=self._safe_filename(original_name),
                        title=self._safe_filename(ex_title),
                        artist=self._safe_filename(ex_artist),
                        album=self._safe_filename(ex_album),
                        year=self._safe_filename(str(ex_year)),
                    ) + '.mp3'
                except (KeyError, ValueError, IndexError):
                    output_filename = f"VK_{i+1:03d}_custom.mp3"

                output_file = os.path.join(self.output_dir, output_filename)

                # --- CUT FRAGMENT ---
                if self.settings['methods'].get('cut_fragment', False):
                    cut_pos_percent = self.settings.get('cut_position_percent', 50)
                    cut_dur = self.settings.get('cut_duration', 2)
                    duration = self._get_duration(current_input)
                    if duration > 0:
                        cut_start = max(0, (duration * cut_pos_percent / 100) - (cut_dur / 2))
                        cut_end = min(duration, cut_start + cut_dur)

                        part1 = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
                        part1.close()
                        temp_files.append(part1.name)
                        part2 = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
                        part2.close()
                        temp_files.append(part2.name)

                        s1, _ = self._safe_subprocess_run(
                            ['ffmpeg', '-i', current_input, '-t', str(cut_start),
                             '-c:a', 'libmp3lame', '-q:a', '2', '-y', part1.name], "cut part 1")
                        s2, _ = self._safe_subprocess_run(
                            ['ffmpeg', '-i', current_input, '-ss', str(cut_end),
                             '-c:a', 'libmp3lame', '-q:a', '2', '-y', part2.name], "cut part 2")

                        if s1 and s2:
                            concat_list = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
                            concat_list.write(f"file '{part1.name}'\nfile '{part2.name}'")
                            concat_list.close()
                            temp_files.append(concat_list.name)
                            cut_result = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
                            cut_result.close()
                            temp_files.append(cut_result.name)
                            sc, _ = self._safe_subprocess_run(
                                ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', concat_list.name,
                                 '-c:a', 'libmp3lame', '-q:a', '2', '-y', cut_result.name], "concat parts")
                            if sc and self._verify_mp3(cut_result.name):
                                current_input = cut_result.name

                # --- TRIM SILENCE ---
                if self.settings['methods'].get('trim_silence', False):
                    trim_dur = self.settings.get('trim_duration', 5)
                    trim_temp = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
                    trim_temp.close()
                    temp_files.append(trim_temp.name)
                    success, _ = self._safe_subprocess_run(
                        ['ffmpeg', '-i', current_input, '-ss', str(trim_dur),
                         '-c:a', 'libmp3lame', '-q:a', '2', '-y', trim_temp.name], "trim silence")
                    if success and self._verify_mp3(trim_temp.name):
                        current_input = trim_temp.name

                # --- MERGE ---
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

                # --- VK INFRASONIC ---
                if self.settings['methods'].get('vk_infrasonic', False):
                    duration = self._get_duration(current_input)
                    if duration and duration > 0:
                        sample_rate = self._get_sample_rate(current_input) or 44100

                        amplitude = self.settings.get('vk_infrasonic_amplitude', 0.35)
                        if self.settings.get('vk_infrasonic_adaptive_amplitude', True):
                            peak = self._get_peak_amplitude(current_input)
                            if peak > 0:
                                max_safe_amp = max(0.05, 0.98 - peak)
                                amplitude = min(amplitude, max_safe_amp)
                                print(f"[VK Infrasonic] Adaptive amplitude: {amplitude:.3f} (peak={peak:.3f})")

                        self.settings['vk_infrasonic_amplitude'] = amplitude
                        expr = self._get_vk_infrasonic_expr(self.settings)

                        vk_temp = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
                        vk_temp.close()
                        temp_files.append(vk_temp.name)

                        fc = (
                            f"[0:a]volume=0.97,asplit[main]; "
                            f"aevalsrc='{expr}':s={sample_rate}:c=stereo[infra]; "
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
                        
                        print(f"[VK Infrasonic] Mode: {self.settings.get('vk_infrasonic_mode', 'modulated')}, "
                            f"Freq: {self.settings.get('vk_infrasonic_freq', 18)} Hz, "
                            f"Amp: {amplitude:.3f}")
                        
                        success_vk, result_vk = self._safe_subprocess_run(cmd_vk, "vk infrasonic", allow_fail=False)
                        if success_vk and self._verify_mp3(vk_temp.name):
                            current_input = vk_temp.name
                            print(f"[VK Infrasonic] Successfully applied")
                        else:
                            print(f"[VK Infrasonic] Failed, skipping this method")
                            if result_vk:
                                print(f"  Error: {self._extract_ffmpeg_error(result_vk.stderr)}")

                # --- ULTRASONIC ---
                ultrasonic_temp = None
                if self.settings['methods'].get('ultrasonic_noise', False):
                    ultrasonic_temp = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
                    ultrasonic_temp.close()
                    temp_files.append(ultrasonic_temp.name)
                    freq = self.settings.get('ultrasonic_freq', 21000)
                    level = self.settings.get('ultrasonic_level', 0.001)
                    duration = self._get_duration(current_input)
                    if duration > 0:
                        self._safe_subprocess_run(
                            ['ffmpeg', '-f', 'lavfi',
                             '-i', f'sine=frequency={freq}:sample_rate=44100:duration={duration}',
                             '-af', f'volume={level}', '-y', ultrasonic_temp.name],
                            "ultrasonic", allow_fail=True)

                # --- MAIN PROCESSING ---
                filters = self._build_filters(current_input)
                cmd = ['ffmpeg', '-i', current_input]

                if ultrasonic_temp and os.path.exists(ultrasonic_temp.name) and os.path.getsize(ultrasonic_temp.name) > 0:
                    cmd.extend(['-i', ultrasonic_temp.name])
                    input_count = 2
                    mix_filter = "[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0"
                    filters = f"{mix_filter},{filters}" if filters else mix_filter
                else:
                    input_count = 1

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
                elif self.settings.get('preserve_cover') and track_info.cover_data:
                    cover_ext = track_info.cover_mime.split('/')[1] if '/' in track_info.cover_mime else 'jpg'
                    cover_temp = tempfile.NamedTemporaryFile(suffix=f'.{cover_ext}', delete=False)
                    cover_temp.write(track_info.cover_data)
                    cover_temp.close()
                    temp_files.append(cover_temp.name)
                    cover_source_path = cover_temp.name

                if cover_source_path:
                    cmd.extend(['-i', cover_source_path])
                    cmd.extend(['-map', '0:a', '-map', f'{input_count}:v'])
                    cmd.extend(['-c:v', 'mjpeg', '-q:v', '2', '-disposition:v', 'attached_pic'])
                else:
                    cmd.extend(['-map', '0:a'])

                if filters:
                    cmd.extend(['-af', filters])

                if self.settings['rename_files']:
                    titles = ["Track", "Song", "Melody", "Rhythm", "Harmony", "Beat", "Flow", "Vibe", "Sound", "Wave"]
                    title_to_use = f"{random.choice(titles)} {random.randint(1, 999)}"
                else:
                    title_to_use = self.metadata['title'] or (track_info.title if self.settings['preserve_metadata'] else "")

                if self.settings.get('reupload', False) and title_to_use:
                    addon = self.settings.get('reupload_text', '(REUPLOAD)')
                    if self.settings.get('reupload_pos', 'after') == 'before':
                        title_to_use = f"{addon} {title_to_use}"
                    else:
                        title_to_use = f"{title_to_use} {addon}"
                if title_to_use:
                    cmd.extend(['-metadata', f'title={title_to_use}'])

                artist = self.metadata['artist'] or (track_info.artist if self.settings['preserve_metadata'] else "")
                if artist:
                    cmd.extend(['-metadata', f'artist={artist}'])
                album = self.metadata['album'] or (track_info.album if self.settings['preserve_metadata'] else "")
                if album:
                    cmd.extend(['-metadata', f'album={album}'])
                year = self.metadata['year'] or (track_info.year if self.settings['preserve_metadata'] else "")
                if year:
                    cmd.extend(['-metadata', f'date={year}'])
                genre = self.metadata['genre'] or (track_info.genre if self.settings['preserve_metadata'] else "")
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
                            print(f"Error applying ID3 padding: {e}")
                    if self.settings['methods'].get('reorder_tags', False):
                        try:
                            self._reorder_id3_tags(output_file)
                        except Exception as e:
                            print(f"Error reordering tags: {e}")
                    if self.settings['methods'].get('broken_duration', False):
                        try:
                            self._apply_broken_duration(output_file, self.settings.get('broken_type', 0))
                        except Exception as e:
                            print(f"Error applying broken duration: {e}")

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
                        print(f"FFmpeg error: {self._extract_ffmpeg_error(result.stderr)}")

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
        try:
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
        except Exception as e:
            print(f"ID3 padding attack failed: {e}")

    def _reorder_id3_tags(self, file_path):
        try:
            audio = MP3(file_path)
            if audio.tags:
                audio.tags.update_to_v23()
                audio.save()
        except Exception as e:
            print(f"Error reordering tags: {e}")

    def _apply_broken_duration(self, file_path, bug_type):
        try:
            try:
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
            except Exception as e:
                print(f"TLEN manipulation failed: {e}")

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

        except Exception as e:
            print(f"Broken duration failed: {e}")