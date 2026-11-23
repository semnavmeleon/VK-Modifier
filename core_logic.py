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
from mutagen import File as MutagenFile
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, TCON, APIC, TLEN, TXXX, PRIV, USLT

FORMAT_CODECS = {
    'mp3': 'libmp3lame', 'flac': 'flac', 'wav': 'pcm_s16le', 'ogg': 'libvorbis',
    'aac': 'aac', 'm4a': 'aac', 'wma': 'wmav2', 'opus': 'libopus',
    'aiff': 'pcm_s16be', 'alac': 'alac', 'wv': 'wavpack', 'ape': 'ape',
    'tta': 'tta', 'ac3': 'ac3', 'dts': 'dts', 'mp2': 'mp2', 'mpc': 'mpc',
    'spx': 'libspeex', 'amr': 'libopencore_amrnb', 'au': 'pcm_s16be',
    'mka': 'libvorbis', 'oga': 'flac', 'caf': 'pcm_s16le', 'shn': 'shorten',
}

# Форматы, для которых в ffmpeg нет энкодера (только декодеры)
NON_ENCODABLE_FORMATS = {'ape', 'mpc', 'shn'}

# Обход VK перестаёт работать, если поддельная длительность оказывается меньше
# часа, поэтому «большие» типы не опускаются ниже _MIN_FAKE_SEC.
_MIN_FAKE_SEC = 3600
_MAX_FAKE_SEC = 86400
# ffmpeg считает длительность по Xing чуть короче заявленной (сам Xing-кадр не
# аудиоданные, плюс стартовое смещение): ровно 3600 с он отдаёт как 3599.97.
# Запас в секунду гарантирует, что пол в один час не будет провален на доли.
_FLOOR_GUARD_SEC = 1
# Типы, для которых пол в один час обязателен. «Очень маленькая» (1) и
# «Нулевая» (5) намеренно дают крошечные значения и в список не входят.
_BIG_FAKE_TYPES = frozenset({0, 2, 3, 4, 6})

# Таблицы частот дискретизации по версии MPEG (индекс — sr_index из заголовка).
# У MPEG1 в кадре 1152 сэмпла, у MPEG2/2.5 — 576: без этого поддельная
# длительность на 22050/24000/16000/11025/8000 Гц уходит ровно вдвое.
_MP3_SAMPLE_RATES = {
    3: (44100, 48000, 32000),   # MPEG1
    2: (22050, 24000, 16000),   # MPEG2
    0: (11025, 12000, 8000),    # MPEG2.5
}


def _mp3_frame_geometry(data, frame_start):
    """Возвращает (sample_rate, samples_per_frame) по заголовку аудиофрейма."""
    if (frame_start + 4 <= len(data)
            and data[frame_start] == 0xFF
            and (data[frame_start + 1] & 0xE0) == 0xE0):
        hdr = int.from_bytes(data[frame_start:frame_start + 4], 'big')
        version = (hdr >> 19) & 0x03
        sr_index = (hdr >> 10) & 0x03
        rates = _MP3_SAMPLE_RATES.get(version)
        if rates and sr_index < 3:
            return rates[sr_index], (1152 if version == 3 else 576)
    return 44100, 1152


def _fake_duration_sec(bug_type, custom_sec):
    """Целевая поддельная длительность в секундах для выбранного типа."""
    try:
        custom_sec = float(custom_sec)
    except (TypeError, ValueError):
        custom_sec = _MIN_FAKE_SEC
    floor = _MIN_FAKE_SEC + _FLOOR_GUARD_SEC
    if bug_type == 0:        # Очень большая
        return random.uniform(6 * 3600, _MAX_FAKE_SEC)
    if bug_type == 1:        # Очень маленькая
        return random.uniform(0.1, 3.0)
    if bug_type == 2:        # Случайная
        return random.uniform(floor, _MAX_FAKE_SEC)
    if bug_type == 3:        # Максимум
        return float(_MAX_FAKE_SEC)
    if bug_type == 4:        # Пользовательская
        return min(max(custom_sec, floor), _MAX_FAKE_SEC)
    if bug_type == 5:        # Нулевая
        return 0.0
    return float(_MAX_FAKE_SEC)  # 6 — Переполнение и всё неизвестное


# Общий лок и реестр зарезервированных имён: параллельные воркеры
# не должны выбирать один и тот же выходной путь
_PATH_LOCK = threading.Lock()
_RESERVED_PATHS = set()

_RANDOM_TITLES = [
    "очень красивый треугольный мальчик",
    "Даниил Глубоких наступил на шиншиллу за это он попадет в ад",
    "паукообразная сучила",
    "нет нахуй это перебор",
    "кто успел тот увидел",
    "вскипяти ближнего своего",
    '"хачи сплошь и рядом" (с) Алина Скрижали',
    "кто такой мать твою Мики Рурка",
    "скрипалей облучила радиация",
    "выстрели в дон жуана",
    "пингвины в бибирево",
    "айсгергерт бежал трусцой сквозь лес",
    "маканова опухоль",
    "неконтролируемый гербарий",
    "хлеб из шпица",
    "гектор на радиоуправлении",
    "трассировка лучей в Doodle Jump",
    "играй в Tekken пока ноги не откажут",
    "бурлеск на горизонте",
    "залезь на окно и очень быстро слезь",
    "иди вари яйца и меньше базарь ты заебал всех",
    "советское пространство",
    "выстрел в тимоти шаламэта из гаубицы",
    "Блюз на блюде",
    "ритузы людвига",
    "тыквенный бенджамин",
    "реверсивный грибник",
    "король синегузлый",
    "привет ребят",
    "здоров пацантрэ",
    "кастрированный хопкинс",
    "метай копье в лану дель рэй",
    "аквамэн на выгуле",
    "спустил с поводка пенилопу",
    "клубняк про мать",
    "убил тома круза киянкой",
    "непосредственно в пиздецах",
    "ногайцы флексят",
    "натяжное небо",
    "нейтрализация марджанджи",
    "я в хлеву)",
    "беззащитный мангал",
    "видео - компиляция где оленя подсбили",
    "пожалуйста намочи манту",
    "нет не верь ей намочи манту",
    "не сиди вконтакте это гиблое дело",
    "душа вышла из пираньи",
    "призрак пираньи",
    "возведи в степень ближнего своего",
    "стреляй в плесень из дробовика",
    "Арнэлла Маргания стреляет из пушки ядрами",
    "играй в бадминтон - но не проеби валанчик",
    "Аслан на сигвее катается",
    "пицца со вкусом живой лошади",
    "не родись скотиной",
    "торговал ядом",
    "педерасты едят греческий салат",
    "Эштон Ветлугин. Все о саморазвитии и бизнесе.",
    'если вы живете в сибири, но у вас фамилия не "пингвинюк" - вы не живете в сибири.',
    "кулаки марьяны",
    "я загадал жестко попиздиться с кенгуру в рукопашном боб",
    "боб)",
    "вокруг степана",
    "никита по - флотски",
    "эбола в варенике",
    "матвей в бутылке",
    "Ебическая сила трения",
    "Скуф мирового значения",
    "Щипач ануса",
    "Подруга пенисной вены",
    "Анти минетная установка",
    "СЕКСНЕИЗБЕЖЕН",
    "мэр пизды",
    "содомия в детдоме",
    "Адмирал Куни",
    "СильвестрСтраппоне",
    "Вагинальный Архитектор",
    "Повелитель Клитора",
    "Вагинальный террорист",
    "Генерал Членосос",
    "Мастурбатор-джедай",
    "Властелин ануса",
    "Трахальщик гусей",
    "Королева Менструаций",
    "Дрочила-убийца",
    "ДрочилаВторогоШанса",
    "Лякурва де блядье",
    "Одухворитель плоти",
    "Узурпатор семени",
    "Вагинальные опарыши",
    "Мраморный минет",
    "Анальный кариес",
    "Анальный тромбоз",
    "Сифилис в бикини",
    "Царь-геморой",
    "Гнойный пирожок",
    "Император простатита",
    "Кишечные опилки",
    "Кишечные опарыши",
    "Боярин белых выделений",
    "Легионер лобкового тифа",
    "Крипто-герпес",
    "Трипперный бард",
    "БКБезКонтрацепции",
    "Опричник гонорейный",
    "Херсонский арбуз",
    "ГазмясОМСК",
    "КозийКИЗЯК",
    "ЕВРЕЙтор",
    "СионскийБратокВКепке",
    "ГопакНаПоловинку",
    "ЧумацкийШлях",
    "Реквием по Чечне",
    "ГенКуколда",
    "Корней Педофил",
    "АнальныйФилософ",
    "АрбузныйТерапевт",
    "ЕврейскийДроид",
    "ГуруКуколдов",
    "Спермотозоид возмездия",
    "Отчиназес",
    "Пашацераптор",
    "ДикПиковая Дама",
    "Сквиртонит",
    "Ким Член мыл",
    "Глиномесси",
    "Анальдо",
    "Господин Сасун",
    "Дикпиковый валет",
    "Призрак шептуна",
    "Шестнадцати клапанное уебище",
    "Горилкофил",
    "Пиздюльтерьер",
    "Аркадий передозов",
    "ПИЗДАГЛАЗЫЙ МОНГОЛ",
    "Крымнашёл",
    "Донбасский Борщ",
    "Чебурашка-Сепаратист",
    "Узбекский Пловец",
    "Дрочер Нации",
    "Орден Дрочилы",
    "Запорный Богатырь",
    "Крымский копрофил",
    "Некрофил оптимист",
    "Святая сперма",
    "Проказа в пельменях",
    "Бздюх всемогущий",
    "Инквизитор импотенции",
    "хуямайнер",
    "карлик кадырьявый",
    "пиздатый шмель",
    "Скурившийся скоробей",
    "хер победивший",
    "Минетчик с причудами",
    "Витязь вшивый",
    "Граф гнилозубов",
    "Серёга Заикалов",
    "Никита по вызову",
    "Никита пофлотский",
]


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
            f = MutagenFile(self.file_path)
            if f and hasattr(f, 'info'):
                if hasattr(f.info, 'length'):
                    self.duration_sec = f.info.length
                if hasattr(f.info, 'bitrate'):
                    self.bitrate = f.info.bitrate // 1000
                if hasattr(f.info, 'sample_rate'):
                    self.sample_rate = f.info.sample_rate
        except Exception:
            pass
        try:
            f = MutagenFile(self.file_path, easy=True)
            if f:
                self.title  = (f.get('title',  ['']) or [''])[0]
                self.artist = (f.get('artist', ['']) or [''])[0]
                self.album  = (f.get('album',  ['']) or [''])[0]
                self.year   = (f.get('date',   ['']) or [''])[0]
                self.genre  = (f.get('genre',  ['']) or [''])[0]
        except Exception:
            pass
        try:
            tags = ID3(self.file_path)
            for key in tags:
                if key.startswith('APIC'):
                    self.cover_data = tags[key].data
                    self.cover_mime = tags[key].mime
                    break
        except Exception:
            pass
        if not self.cover_data:
            try:
                from mutagen.flac import FLAC
                flac = FLAC(self.file_path)
                if flac.pictures:
                    self.cover_data = flac.pictures[0].data
                    self.cover_mime = flac.pictures[0].mime_type
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
            # энкодер dca в ffmpeg помечен экспериментальным
            args.extend(['-codec:a', codec, '-strict', '-2', '-b:a', '1536k'])
        elif self.output_format == 'mp2':
            args.extend(['-codec:a', codec, '-b:a', '256k'])
        elif self.output_format == 'mpc':
            args.extend(['-codec:a', codec, '-q:a', '7'])
        elif self.output_format == 'spx':
            # libspeex поддерживает только 8/16/32 кГц — иначе ошибка на любом обычном треке
            args.extend(['-codec:a', codec, '-ar', '16000', '-ac', '1', '-q:a', '8'])
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

        if self.output_format in NON_ENCODABLE_FORMATS:
            self.queue.put(('file_done', file_path, False, ""))
            self.queue.put(('error',
                f"Ошибка конвертации {os.path.basename(file_path)}: формат "
                f"{self.output_format.upper()} не поддерживается — в ffmpeg нет энкодера"))
            return

        try:
            base_name = os.path.splitext(os.path.basename(file_path))[0]

            with _PATH_LOCK:
                output_name = f"{base_name}.{self.output_format}"
                output_path = os.path.join(self.output_dir, output_name)
                counter = 1
                while os.path.exists(output_path) or output_path in _RESERVED_PATHS:
                    output_name = f"{base_name}_{counter}.{self.output_format}"
                    output_path = os.path.join(self.output_dir, output_name)
                    counter += 1
                _RESERVED_PATHS.add(output_path)

            args = self._get_ffmpeg_args(file_path, output_path)
            result = subprocess.run(args, capture_output=True, stdin=subprocess.DEVNULL, encoding='utf-8', errors='ignore', timeout=1800)

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
        self._run_temp_files = []
        self._xing_conflict_warned = False

    @staticmethod
    def _safe_filename(s):
        return re.sub(r'[\\/*?:"<>|]', '_', str(s)).strip() or '_'

    @staticmethod
    def _read_original_tags(file_path: str) -> dict:
        try:
            f = MutagenFile(file_path, easy=True)
            if f is None:
                return {}
            return {
                'title':  (f.get('title',  ['']) or [''])[0],
                'artist': (f.get('artist', ['']) or [''])[0],
                'album':  (f.get('album',  ['']) or [''])[0],
                'year':   (f.get('date',   ['']) or [''])[0],
                'genre':  (f.get('genre',  ['']) or [''])[0],
            }
        except Exception:
            return {}

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

    def _safe_subprocess_run(self, cmd, description="", allow_fail=False, timeout=900):
        try:
            result = subprocess.run(cmd, capture_output=True, stdin=subprocess.DEVNULL, encoding='utf-8', errors='ignore', timeout=timeout)
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
            '-map', '[out]'
        ] + self._temp_codec() + ['-y', output_path]
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
            max_samples = 1 << 20  # ~23.8 с — предел размера FFT
            cmd = ['ffmpeg', '-i', audio_path, '-vn', '-f', 'f32le',
                   '-ac', '1', '-ar', str(sample_rate), 'pipe:1', '-y']
            result = subprocess.run(cmd, capture_output=True, stdin=subprocess.DEVNULL, timeout=300)
            if result.returncode != 0 or not result.stdout:
                return self._analyze_spectrum_ffmpeg(audio_path, bands)

            samples = np.frombuffer(result.stdout, dtype=np.float32)
            if len(samples) < sample_rate:
                return self._analyze_spectrum_ffmpeg(audio_path, bands)

            samples = np.asarray(samples[:max_samples], dtype=np.float64)
            # Окно Ханна убирает утечку спектра от прямоугольного окна
            samples = samples * np.hanning(len(samples))
            fft_size = 1 << (len(samples) - 1).bit_length()
            spectrum = np.abs(np.fft.rfft(samples, n=fft_size)) ** 2
            freqs = np.fft.rfftfreq(fft_size, d=1.0 / sample_rate)

            band_energies = []
            for center in bands:
                low = center / 1.4
                high = center * 1.4
                mask = (freqs >= low) & (freqs <= high)
                if mask.any():
                    power = float(np.mean(spectrum[mask]))
                    if power > 0:
                        # spectrum — мощность, поэтому 10*log10 (не от sqrt)
                        power_db = 10 * np.log10(power)
                        if power_db > -80:
                            band_energies.append((center, power_db))
            return band_energies or None

        except ImportError:
            return self._analyze_spectrum_ffmpeg(audio_path, bands)
        except Exception:
            return self._analyze_spectrum_ffmpeg(audio_path, bands)

    def _analyze_spectrum_ffmpeg(self, audio_path, bands):
        band_energies = []
        for center_freq in bands:
            width = max(2, center_freq // 4)
            cmd = [
                'ffmpeg', '-i', audio_path,
                '-af', f'bandpass=f={center_freq}:width={width}:csg=1,astats=metadata=0',
                '-f', 'null', '-'
            ]
            success, result = self._safe_subprocess_run(cmd, f"analyzing {center_freq}Hz", allow_fail=True)
            if success and result and result.stderr:
                m = re.search(r'RMS level dB:\s*(-?[\d.]+|-?inf)', result.stderr)
                if m and 'inf' not in m.group(1):
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

        filters.append("alimiter=limit=0.98:attack=5:release=50")
        return ", ".join(filters)

    def _build_midside_filter(self, mid_eq_gain=-3, side_eq_gain=2):
        # mlev/slev у stereotools — ЛИНЕЙНЫЕ множители (диапазон [0.015625, 64], default 1),
        # поэтому dB корректно переводим в линейные и клампим по диапазону фильтра
        mid_lin  = 10 ** (float(mid_eq_gain) / 20)
        side_lin = 10 ** (float(side_eq_gain) / 20)
        mid_lin  = max(0.015625, min(64.0, mid_lin))
        side_lin = max(0.015625, min(64.0, side_lin))
        return f"stereotools=mlev={mid_lin:.4f}:slev={side_lin:.4f}"

    def _build_psychoacoustic_noise_filter(self, intensity=0.0003):
        # intensity трактуем как линейную амплитуду шума (диапазон GUI 1e-5..5e-3)
        amplitude = max(1e-5, min(0.05, float(intensity)))
        return self._build_shaped_noise_filter(amplitude=amplitude)

    def _build_saturation_filter(self, drive=1.5, mix=0.15):
        mix_clamped = max(0.0, min(1.0, mix))
        drive_clamped = max(1.0, min(5.0, drive))
        return (
            f"acompressor=threshold=-12dB:ratio=4:attack=1:release=50:makeup=2,"
            f"acrusher=level_in={drive_clamped:.2f}:level_out=1:bits=14:mode=log:mix={mix_clamped:.2f}"
        )

    def _build_micro_pitch_filter(self, cents, audio_path=None):
        sr = self._get_sample_rate(audio_path) if audio_path else 44100
        shifted_sr = round(sr * (2 ** (cents / 1200)))
        # asetrate+aresample меняют и питч, и длительность — возвращаем темп через atempo
        tempo_comp = 2 ** (-cents / 1200)
        return (f"asetrate={shifted_sr},aresample={sr}:resampler=soxr:precision=28,"
                f"atempo={tempo_comp:.6f}")

    def _build_temporal_jitter_filter(self, intensity=0.002, frequency=0.5):
        freq_clamped = max(0.1, min(2.0, frequency))
        delay = max(0.1, min(5.0, intensity * 2000))
        decay = min(0.3, intensity * 50)
        return f"aphaser=type=t:delay={delay:.2f}:decay={decay:.3f}:speed={freq_clamped:.2f}:out_gain=0.9"

    @staticmethod
    def _atempo_factors(speed):
        # одиночный atempo поддерживает 0.5..100; вне диапазона раскладываем в цепочку
        factors = []
        s = max(1e-3, float(speed))
        while s < 0.5:
            factors.append(0.5)
            s /= 0.5
        while s > 100.0:
            factors.append(100.0)
            s /= 100.0
        factors.append(s)
        return factors

    def _measure_loudnorm(self, audio_path, target):
        try:
            cmd = ['ffmpeg', '-i', audio_path,
                   '-af', f'loudnorm=I={target:.1f}:TP=-1.5:LRA=11:print_format=json',
                   '-f', 'null', '-']
            success, result = self._safe_subprocess_run(cmd, "loudnorm measure", allow_fail=True)
            if not (success and result and result.stderr):
                return None
            m = re.search(r'\{[^{}]*"input_i"[^{}]*\}', result.stderr, re.S)
            if not m:
                return None
            stats = json.loads(m.group(0))
            if float(stats.get('input_i', -70.0)) <= -69.9:
                return None
            return stats
        except Exception:
            return None

    def _build_loudnorm_filter(self, target, audio_path):
        # Двухпроходный linear-режим: без измерений loudnorm работает динамически
        # (pumping) и всегда ресемплит выход в 192 кГц
        stats = None
        if audio_path and os.path.exists(audio_path):
            stats = self._measure_loudnorm(audio_path, target)
        if stats:
            try:
                return (f"loudnorm=I={target:.1f}:TP=-1.5:LRA=11"
                        f":measured_I={float(stats['input_i']):.2f}"
                        f":measured_TP={float(stats['input_tp']):.2f}"
                        f":measured_LRA={float(stats['input_lra']):.2f}"
                        f":measured_thresh={float(stats['input_thresh']):.2f}"
                        f":offset={float(stats['target_offset']):.2f}"
                        f":linear=true")
            except (KeyError, TypeError, ValueError):
                pass
        return f"loudnorm=I={target:.1f}:TP=-1.5:LRA=11"

    @staticmethod
    def _spread_frequencies(pool, n, min_octave_gap=0.5, rng=None):
        import math
        rng = rng or random
        candidates = list(pool)
        rng.shuffle(candidates)

        def _pick(gap):
            sel = []
            for f in candidates:
                if not any(abs(math.log2(f / s)) < gap for s in sel):
                    sel.append(f)
                if len(sel) >= n:
                    break
            return sel

        selected = _pick(min_octave_gap)
        if len(selected) < n:
            selected = _pick(min_octave_gap / 2)
        if len(selected) < n:
            rest = [f for f in candidates if f not in selected]
            selected.extend(rest[:n - len(selected)])
        while len(selected) < n:
            base = rng.choice(pool)
            jitter = rng.uniform(0.97, 1.03)
            selected.append(round(base * jitter))
        return selected

    def _find_spectral_peaks(self, audio_path, n, min_octave_gap=0.5):
        freq_pool = [
             60,   80,  100,  120,  160,  200,  250,  315,
            400,  500,  630,  800, 1000, 1250, 1600, 2000,
           2500, 3150, 4000, 5000, 6300, 7000, 8000, 9000,
          10000, 11000, 12500, 14000, 16000, 18000, 19000, 20000,
        ]
        energies = self._analyze_spectrum(audio_path, freq_pool)
        if not energies:
            return None
        # Убираем общий наклон спектра (регрессия уровня по log-частоте), иначе бас всегда громче
        if len(energies) >= 3:
            xs = [math.log2(f) for f, _ in energies]
            ys = [db for _, db in energies]
            mx = sum(xs) / len(xs)
            my = sum(ys) / len(ys)
            sxx = sum((x - mx) ** 2 for x in xs)
            slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx else 0.0
            energies = [(f, db - (my + slope * (math.log2(f) - mx))) for f, db in energies]
        energies.sort(key=lambda x: x[1], reverse=True)
        # Берём самые выступающие над трендом частоты, разнося их по октавам; если не хватает — ослабляем разнос
        picked = []
        for gap in (min_octave_gap, min_octave_gap / 2, 0.0):
            for freq, _ in energies:
                if freq in picked or not all(abs(math.log2(freq / p)) >= gap for p in picked):
                    continue
                picked.append(freq)
                if len(picked) >= n:
                    return picked
        return picked or None

    def _build_asendcmd_filter(self, named_notches, audio_path):
        duration = self._get_duration(audio_path) if audio_path else 0
        if not duration or duration <= 0:
            return None
        lfo_freq = 0.15
        lfo_depth = 1.5
        dt = 0.4
        lines = []
        t = 0.0
        n = len(named_notches)
        while t <= duration + dt:
            for i, (label, freq, base_att, q) in enumerate(named_notches):
                phase = i * (2 * math.pi / n)
                mod = lfo_depth * math.sin(2 * math.pi * lfo_freq * t + phase)
                gain = min(-0.3, -(base_att + mod))
                lines.append(f"{t:.1f} {label} gain {gain:.2f};")
            t += dt
        return self._write_cmd_file(lines)

    def _write_cmd_file(self, lines):
        """Пишет файл команд для asendcmd и возвращает строку фильтра (или None).

        Файл регистрируется во временных файла́х прогона, чтобы run() его удалил.
        """
        if not lines:
            return None
        try:
            tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8')
            tmp.write('\n'.join(lines))
            tmp.close()
            getattr(self, '_run_temp_files', []).append(tmp.name)
            safe = tmp.name.replace('\\', '/').replace(':', '\\:')
            return f"asendcmd=filename='{safe}'"
        except Exception:
            return None

    @staticmethod
    def _build_shaped_noise_filter(amplitude=0.0022):
        # random() в aeval даёт одинаковую последовательность для всех каналов,
        # поэтому шум строится хэшем от номера сэмпла (n) и канала (ch): каналы независимые
        def hash_noise(a, b):
            return f"(mod(abs(sin(n*{a}+ch*{b}))*43758.5453\\,1)-0.5)"
        amp = max(1e-6, float(amplitude))
        noise = f"{amp * 0.66:.8f}*{hash_noise(12.9898, 78.233)}+{amp * 0.34:.8f}*{hash_noise(39.3468, 11.135)}"
        return f"aeval=val(ch)+{noise}:c=same"

    def _build_spectral_jitter_filter(self, num_notches=5, max_attenuation=15, fixed_frequencies=None,
                                       fixed_attenuation=None, manual_config=None,
                                       seed=None, min_octave_gap=0.5,
                                       audio_path=None, adaptive=False,
                                       compensate_boost=False, temporal_mod=False):
        import math
        # Локальный RNG: глобальный random.seed() в параллельных тредах
        # менял бы «случайные» значения у других файлов (тайтлы и т.п.)
        rng = random.Random(seed)

        filters = []
        freq_pool = [
             60,   80,  100,  120,  160,  200,  250,  315,
            400,  500,  630,  800, 1000, 1250, 1600, 2000,
           2500, 3150, 4000, 5000, 6300, 7000, 8000, 9000,
          10000, 11000, 12500, 14000, 16000, 18000, 19000, 20000,
        ]

        # Единый список нотчей (частота, ослабление dB, Q/ширина) для всех режимов
        notches = []

        if isinstance(manual_config, dict) and manual_config.get('mode') == 'manual':
            frequencies  = manual_config.get('frequencies', [])
            attenuations = manual_config.get('attenuations', [])
            widths       = manual_config.get('widths', [])
            default_width = manual_config.get('fixed_width', 2.0)

            for i, freq in enumerate(frequencies):
                att   = attenuations[i] if i < len(attenuations) else max_attenuation
                width = widths[i]       if i < len(widths)       else default_width
                notches.append((freq, att, width))

        elif fixed_frequencies:
            default_width = 2.0
            if isinstance(manual_config, dict) and 'fixed_width' in manual_config:
                default_width = manual_config['fixed_width']

            if isinstance(manual_config, dict) and 'fixed_attenuation' in manual_config:
                att = manual_config['fixed_attenuation']
            elif fixed_attenuation is not None:
                att = fixed_attenuation
            else:
                att = max_attenuation

            for freq in fixed_frequencies:
                notches.append((freq, att, default_width))

        else:
            num_notches_int = int(round(num_notches))
            if num_notches_int <= 0:
                return ""

            if adaptive and audio_path and os.path.exists(audio_path):
                selected = self._find_spectral_peaks(audio_path, num_notches_int, min_octave_gap) or []
                # Если пиков не хватило, добираем равномерно распределёнными частотами
                if len(selected) < num_notches_int:
                    extra = [f for f in self._spread_frequencies(freq_pool, num_notches_int, min_octave_gap, rng)
                             if f not in selected]
                    selected += extra[:num_notches_int - len(selected)]
            else:
                selected = self._spread_frequencies(freq_pool, num_notches_int, min_octave_gap, rng)

            for freq in selected:
                att_val = fixed_attenuation if fixed_attenuation is not None \
                          else rng.uniform(max_attenuation / 2, max_attenuation)
                q = max(0.8, min(6.0, 2.0 + 0.4 * math.log2(freq / 1000)))
                q += rng.uniform(-0.2, 0.2)
                notches.append((freq, att_val, q))

        named_notches = []

        for i, (freq, att_val, q) in enumerate(notches):
            if temporal_mod:
                label = f"equalizer@sj{i}"
                filters.append(f"{label}=f={freq}:width_type=q:width={q:.2f}:g=-{att_val:.1f}")
                named_notches.append((label, freq, att_val, q))
            else:
                filters.append(f"equalizer=f={freq}:width_type=q:width={q:.2f}:g=-{att_val:.1f}")

            if compensate_boost:
                boost_freq = min(int(round(freq * 1.5)), 20000)
                bq = max(0.8, min(6.0, 2.0 + 0.4 * math.log2(boost_freq / 1000)))
                filters.append(f"equalizer=f={boost_freq}:width_type=q:width={bq:.2f}:g=0.5")

        result = ", ".join(filters)

        if temporal_mod and named_notches:
            cmd_filter = self._build_asendcmd_filter(named_notches, audio_path)
            if cmd_filter:
                result = cmd_filter + "," + result

        return result

    def _build_fp_jitter_eq(self, rng, audio_path):
        """Плотный банк мелких полосовых коррекций с медленной модуляцией усиления.

        Полосы не перекрываются (ширина ≈ 1.4 шага), поэтому случайные
        усиления не складываются и общий уровень/тембр почти не меняется.
        """
        bands = max(4, min(48, int(self.settings.get('fp_jitter_bands', 20))))
        depth = max(0.2, min(6.0, float(self.settings.get('fp_jitter_depth', 1.5))))
        mod_rate = max(0.01, min(1.0, float(self.settings.get('fp_jitter_mod_rate', 0.1))))

        fmin, fmax = 80.0, 16000.0
        spacing = math.log2(fmax / fmin) / max(bands - 1, 1)
        width = spacing * 1.4

        rows = []
        for i in range(bands):
            f = fmin * 2.0 ** (i * spacing)
            g = rng.uniform(-depth, depth)
            rows.append((f"fpj{i}", f, g))

        chain = ", ".join(
            f"equalizer@{lbl}=f={f:.2f}:width_type=o:width={width:.4f}:g={g:.2f}"
            for lbl, f, g in rows)

        if not self.settings.get('fp_jitter_mod', True):
            return chain
        duration = self._get_duration(audio_path) if audio_path else 0
        if not duration or duration <= 0:
            return chain

        # цель команды обязана быть 'equalizer@<имя>': просто '<имя>' ffmpeg
        # молча игнорирует, а 'equalizer' бьёт только по первому совпадению
        dt = 0.5 if duration <= 300 else 1.0
        lines = []
        for lbl, _f, g in rows:
            rate = rng.uniform(mod_rate * 0.6, mod_rate * 1.7)
            phase = rng.uniform(0, 2 * math.pi)
            t = 0.0
            while t <= duration + dt:
                gain = g + depth * 0.8 * math.sin(2 * math.pi * rate * t + phase)
                lines.append(f"{t:.2f} equalizer@{lbl} gain {gain:.2f};")
                t += dt
        cmd = self._write_cmd_file(lines)
        return f"{cmd},{chain}" if cmd else chain

    def _build_fp_jitter_filter(self, audio_path):
        """Сдвиг временной базы + плотный мелкий EQ.

        Замерено на chromaprint (bench_fingerprint.py): глубокие нотчи
        (7.5–15 дБ) меняют тембр на 4.3 дБ и оставляют сходство отпечатка 0.91,
        тогда как сдвиг временной базы на 0.5% меняет тембр на 0.1 дБ и даёт
        0.80 — ломает отпечаток сильнее почти без слышимых последствий.
        """
        rng = random.Random()
        parts = []

        mode = self.settings.get('fp_jitter_time_mode', 'random')
        if mode not in ('detune', 'tempo'):
            mode = rng.choice(('detune', 'tempo'))

        # знак и величина случайны на каждый трек, но не меньше 40% от заданного
        # максимума — иначе часть файлов получит сдвиг около нуля и не изменится
        if mode == 'detune':
            limit = max(0.05, min(3.0, float(self.settings.get('fp_jitter_detune_pct', 0.6))))
            pct = rng.choice((-1.0, 1.0)) * rng.uniform(limit * 0.4, limit)
            sr = self._get_sample_rate(audio_path) if audio_path else 44100
            parts.append(f"asetrate={sr * (1 + pct / 100.0):.4f},"
                         f"aresample={sr}:resampler=soxr:precision=28")
        else:
            limit = max(0.5, min(10.0, float(self.settings.get('fp_jitter_tempo_pct', 3.0))))
            pct = rng.choice((-1.0, 1.0)) * rng.uniform(limit * 0.4, limit)
            parts.append(f"rubberband=tempo={1 + pct / 100.0:.5f}")

        eq = self._build_fp_jitter_eq(rng, audio_path)
        if eq:
            parts.append(eq)
        return ", ".join(parts) if parts else None

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
        elif mode == 'maximum':
            mod_term = f"(1-{mod_depth}+{mod_depth}*sin(2*PI*{mod_freq}*t))"
            terms = [f"{amp}*{mod_term}*{wave_func(waveform, base_arg)}"]
            for i, h_amp in enumerate(harmonics, start=2):
                if h_amp > 0:
                    h_arg = f"2*PI*{freq * i}*t"
                    if total_phase != 0:
                        h_arg = f"({h_arg}+{total_phase:.4f})"
                    terms.append(f"{amp * h_amp}*{mod_term}*{wave_func(waveform, h_arg)}")
            expr = "+".join(terms)
        else:
            mod_term = f"(1-{mod_depth}+{mod_depth}*sin(2*PI*{mod_freq}*t))"
            expr = f"{amp}*{mod_term}*{wave_func(waveform, base_arg)}"

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
            spec_jitter = self._build_spectral_jitter_filter(
                num_notches, att, fixed_freqs, fixed_att, manual_cfg,
                audio_path=current_input,
                adaptive=self.settings.get('sj_adaptive', False),
                compensate_boost=self.settings.get('sj_boost', False),
                temporal_mod=self.settings.get('sj_temporal', False),
            )
            if spec_jitter:
                filters.append(spec_jitter)

        if self.settings['methods'].get('loudnorm', False):
            target = self.settings.get('loudnorm_target', -14.0)
            ln = self._build_loudnorm_filter(target, current_input)
            if ln:
                filters.append(ln)

        if self.settings['methods'].get('resample_drift', False):
            # дрейф в процентах: старая единица «Гц» давала 1/44100 = 0.002%,
            # то есть метод фактически ничего не делал
            drift_pct = max(-2.0, min(2.0, float(self.settings.get('resample_drift_amount', 0.3))))
            sr = self._get_sample_rate(current_input) if current_input else 44100
            filters.append(f"asetrate={sr * (1 + drift_pct / 100.0):.4f},"
                           f"aresample={sr}:resampler=soxr:precision=28")

        if self.settings.get('drift_variable', False):
            filters.append("vibrato=f=0.08:d=0.002")

        if self.settings.get('micro_pitch', False):
            cents = self.settings.get('micro_pitch_cents', 4.0)
            if self.settings.get('micro_pitch_random_sign', True):
                cents = abs(cents) * random.choice([-1, 1])
            pitch_f = self._build_micro_pitch_filter(cents, audio_path=current_input)
            if pitch_f:
                filters.append(pitch_f)

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
            semitones = max(-12.0, min(12.0, float(self.settings.get('pitch_value', 0))))
            sr = self._get_sample_rate(current_input) if current_input else 44100
            rate = sr * (2 ** (semitones / 12))
            # asetrate+aresample — varispeed: без atempo меняется и длительность
            tempo_comp = 2 ** (-semitones / 12)
            filters.append(f"asetrate={rate:.0f},aresample={sr},atempo={tempo_comp:.6f}")

        if self.settings['methods'].get('speed', False):
            spd = float(self.settings.get('speed_value', 1.0))
            if spd > 0:
                for factor in self._atempo_factors(spd):
                    filters.append(f"atempo={factor:.6f}")
            else:
                self.on_error("Speed: коэффициент должен быть больше 0, метод пропущен")

        if self.settings['methods'].get('eq', False):
            if self.settings.get('eq_type') == 1:
                filters.append("equalizer=f=1000:width_type=o:width=2:g=-4")
                filters.append("equalizer=f=2000:width_type=o:width=2:g=-2")
            elif self.settings.get('eq_type') == 2:
                filters.append("equalizer=f=8000:width_type=o:width=2:g=3")
            else:
                filters.append(f"equalizer=f=1000:width_type=o:width=2:g={-self.settings['eq_value']}")

        # apad (silence) добавляется последним в _compute_filters — после фейда,
        # чтобы фейд приходился на музыку, а не на дописанную тишину
        return filters

    def _temp_ext(self):
        return '.wav' if self.settings.get('lossless_intermediate', False) else '.mp3'

    def _temp_codec(self):
        if self.settings.get('lossless_intermediate', False):
            return ['-c:a', 'pcm_s16le']
        return ['-c:a', 'libmp3lame', '-q:a', '2']

    def _verify_audio(self, file_path):
        if file_path.endswith('.wav'):
            try:
                if not (os.path.exists(file_path) and os.path.getsize(file_path) > 44):
                    return False
                with open(file_path, 'rb') as f:
                    return f.read(4) == b'RIFF'
            except Exception:
                return False
        return self._verify_mp3(file_path)


    def _read_track_metadata(self, file_path, track_info):
        original_name = os.path.splitext(os.path.basename(file_path))[0]
        _need_orig = any(self.metadata.get(f'_append_{k}')
                         for k in ('title', 'artist', 'album', 'year', 'genre'))
        _orig_from_file = self._read_original_tags(file_path) if (
            _need_orig or track_info is None) else {}

        def _ti(key, ti_val):
            return ti_val or _orig_from_file.get(key, '') or ''

        orig = {
            'title':  _ti('title',  track_info.title  if track_info else ''),
            'artist': _ti('artist', track_info.artist if track_info else ''),
            'album':  _ti('album',  track_info.album  if track_info else ''),
            'year':   _ti('year',   str(track_info.year) if track_info else ''),
            'genre':  _ti('genre',  track_info.genre  if track_info else ''),
        }

        def _combined(key, fallback):
            v = self.metadata.get(key, '')
            if v and self.metadata.get(f'_append_{key}') and fallback:
                return fallback + v
            return v or fallback

        return {
            'orig':      orig,
            'ex_title':  _combined('title',  orig['title'] or original_name),
            'ex_artist': _combined('artist', orig['artist']),
            'ex_album':  _combined('album',  orig['album']),
            'ex_year':   _combined('year',   orig['year']),
        }

    def _resolve_output_path(self, i, original_name, ex_title, ex_artist, ex_album, ex_year):
        tpl = self.settings.get('filename_template', 'VK_{n:03d}_custom')
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

        base_fn = os.path.splitext(output_filename)[0]
        with _PATH_LOCK:
            output_file = os.path.join(self.output_dir, output_filename)
            col = 1
            while os.path.exists(output_file) or output_file in _RESERVED_PATHS:
                output_filename = f"{base_fn}_{col}.mp3"
                output_file = os.path.join(self.output_dir, output_filename)
                col += 1
            _RESERVED_PATHS.add(output_file)
        return output_file

    def _apply_cut_fragment(self, current_input, temp_files):
        if not self.settings['methods'].get('cut_fragment', False):
            return current_input
        cut_pos_percent = self.settings.get('cut_position_percent', 50)
        cut_dur = self.settings.get('cut_duration', 2)
        duration = self._get_duration(current_input)
        if duration <= 0:
            return current_input
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
        if not (s1 and s2):
            return current_input
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
            return cut_result.name
        return current_input

    def _apply_trim_silence(self, current_input, temp_files):
        if not self.settings['methods'].get('trim_silence', False):
            return current_input
        trim_dur = self.settings.get('trim_duration', 5)
        threshold = self.settings.get('trim_silence_threshold', -60)
        silence_end = 0.0
        detect_cmd = ['ffmpeg', '-i', current_input, '-af',
                      f'silencedetect=noise={threshold}dB:duration=0.3',
                      '-f', 'null', '-']
        ok, res = self._safe_subprocess_run(detect_cmd, "silence detect", allow_fail=True)
        if ok and res and res.stderr:
            # обрезать можно только ВЕДУЩУЮ тишину: первый интервал должен начинаться у нуля,
            # иначе silence_end может указать на паузу в середине трека
            starts = re.findall(r'silence_start:\s*(-?[\d.]+)', res.stderr)
            ends = re.findall(r'silence_end:\s*([\d.]+)', res.stderr)
            if starts and ends and float(starts[0]) <= 0.3:
                silence_end = float(ends[0])
        if silence_end <= 0:
            return current_input
        silence_end = min(silence_end, trim_dur)
        trim_temp = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
        trim_temp.close()
        temp_files.append(trim_temp.name)
        success, _ = self._safe_subprocess_run(
            ['ffmpeg', '-i', current_input, '-ss', str(silence_end)]
            + self._temp_codec() + ['-y', trim_temp.name], "trim silence")
        if success and self._verify_audio(trim_temp.name):
            return trim_temp.name
        return current_input

    def _apply_merge(self, current_input, temp_files):
        if not (self.settings['methods'].get('merge', False) and self.settings.get('extra_track_path')):
            return current_input
        extra_track = self.settings['extra_track_path']
        if not (os.path.exists(extra_track) and self._verify_mp3(extra_track)):
            return current_input
        norm1 = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
        norm1.close()
        temp_files.append(norm1.name)
        norm2 = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
        norm2.close()
        temp_files.append(norm2.name)
        s1, _ = self._safe_subprocess_run(
            ['ffmpeg', '-i', current_input, '-ar', '44100', '-ac', '2',
             '-c:a', 'pcm_s16le', '-y', norm1.name], "normalize main")
        s2, _ = self._safe_subprocess_run(
            ['ffmpeg', '-i', extra_track, '-ar', '44100', '-ac', '2',
             '-c:a', 'pcm_s16le', '-y', norm2.name], "normalize extra")
        if not (s1 and s2 and os.path.getsize(norm1.name) > 0 and os.path.getsize(norm2.name) > 0):
            return current_input
        concat_list = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8')
        concat_list.write(f"file '{norm1.name.replace(chr(92), '/')}'\n")
        concat_list.write(f"file '{norm2.name.replace(chr(92), '/')}'\n")
        concat_list.close()
        temp_files.append(concat_list.name)
        merge_temp = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
        merge_temp.close()
        temp_files.append(merge_temp.name)
        success, _ = self._safe_subprocess_run(
            ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', concat_list.name]
            + self._temp_codec() + ['-y', merge_temp.name], "concat wav")
        if success and self._verify_audio(merge_temp.name):
            return merge_temp.name
        self._try_alternative_merge(current_input, extra_track, temp_files, merge_temp.name)
        if os.path.exists(merge_temp.name) and self._verify_audio(merge_temp.name):
            return merge_temp.name
        return current_input

    def _apply_overlay(self, current_input, temp_files):
        if not (self.settings['methods'].get('overlay', False) and self.settings.get('overlay_path')):
            return current_input
        extra = self.settings['overlay_path']
        if not os.path.exists(extra):
            return current_input
        overlay_vol = self.settings.get('overlay_volume', 0.5)
        original_volume = max(0.5, 1.0 - overlay_vol * 0.5)
        overlay_temp = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
        overlay_temp.close()
        temp_files.append(overlay_temp.name)
        fc = (
            f"[0:a]volume={original_volume:.4f}[main];"
            f"[1:a]volume={overlay_vol:.4f}[extra];"
            f"[main][extra]amix=inputs=2:duration=first:dropout_transition=0:normalize=0,"
            f"alimiter=limit=0.98:attack=5:release=50[out]"
        )
        cmd = [
            'ffmpeg', '-i', current_input, '-i', extra,
            '-filter_complex', fc, '-map', '[out]',
        ] + self._temp_codec() + ['-y', overlay_temp.name]
        success, _ = self._safe_subprocess_run(cmd, "overlay")
        if success and self._verify_audio(overlay_temp.name):
            return overlay_temp.name
        return current_input

    def _apply_insert_audio(self, current_input, temp_files):
        if not (self.settings['methods'].get('insert_audio', False) and self.settings.get('insert_audio_path')):
            return current_input
        extra = self.settings['insert_audio_path']
        if not os.path.exists(extra):
            return current_input
        insert_pos = self.settings.get('insert_position_sec', 10.0)

        part1 = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
        part1.close()
        temp_files.append(part1.name)
        part2 = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
        part2.close()
        temp_files.append(part2.name)
        extra_norm = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
        extra_norm.close()
        temp_files.append(extra_norm.name)

        # все три части приводим к 44100/стерео — concat-demuxer требует
        # одинаковых параметров потоков, иначе щелчки/рассинхрон на стыках
        s1, _ = self._safe_subprocess_run(
            ['ffmpeg', '-i', current_input, '-t', str(insert_pos), '-ar', '44100', '-ac', '2']
            + self._temp_codec() + ['-y', part1.name], "insert part1")
        s2, _ = self._safe_subprocess_run(
            ['ffmpeg', '-i', current_input, '-ss', str(insert_pos), '-ar', '44100', '-ac', '2']
            + self._temp_codec() + ['-y', part2.name], "insert part2")
        se, _ = self._safe_subprocess_run(
            ['ffmpeg', '-i', extra, '-ar', '44100', '-ac', '2']
            + self._temp_codec() + ['-y', extra_norm.name], "insert normalize extra")
        if not (s1 and s2 and se):
            return current_input

        concat_list = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8')
        concat_list.write(f"file '{part1.name.replace(chr(92), '/')}'\n")
        concat_list.write(f"file '{extra_norm.name.replace(chr(92), '/')}'\n")
        concat_list.write(f"file '{part2.name.replace(chr(92), '/')}'\n")
        concat_list.close()
        temp_files.append(concat_list.name)

        result_temp = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
        result_temp.close()
        temp_files.append(result_temp.name)
        sc, _ = self._safe_subprocess_run(
            ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', concat_list.name]
            + self._temp_codec() + ['-y', result_temp.name], "insert concat")
        if sc and self._verify_audio(result_temp.name):
            return result_temp.name
        return current_input

    def _apply_vk_infrasonic(self, current_input, temp_files):
        if not self.settings['methods'].get('vk_infrasonic', False):
            return current_input
        duration = self._get_duration(current_input)
        if not (duration and duration > 0):
            return current_input
        sample_rate = self._get_sample_rate(current_input) or 44100
        amplitude = self.settings.get('vk_infrasonic_amplitude', 0.35)
        peak = self._get_peak_amplitude(current_input)
        if self.settings.get('vk_infrasonic_adaptive_amplitude', True) and peak > 0:
            # реальный запас громкости до 0.98; без запаса инфразвук добавляется минимально
            max_infra_amp = max(0.02, (0.98 - peak) * 0.9)
            amplitude = min(amplitude, max_infra_amp)
        if peak > 0:
            # громкость музыки подбираем так, чтобы сумма music+infra не вышла за 0.98;
            # тихие треки не глушим (верхняя граница 1.0, а не 0.97)
            original_volume = max(0.3, min(1.0, (0.98 - amplitude) / peak))
        else:
            original_volume = 0.95
        local_vk = {**self.settings, 'vk_infrasonic_amplitude': amplitude}
        expr_l = self._get_vk_infrasonic_expr(local_vk, extra_phase=0.0)
        expr_r = self._get_vk_infrasonic_expr(local_vk, extra_phase=0.5236)
        fade_in = min(1.0, duration * 0.02)
        fade_out = min(1.0, duration * 0.02)
        fade_env = f"min(t/{fade_in:.4f},1)*min(({duration:.4f}-t)/{fade_out:.4f},1)"
        expr_l = f"({fade_env})*({expr_l})"
        expr_r = f"({fade_env})*({expr_r})"
        vk_temp = tempfile.NamedTemporaryFile(suffix=self._temp_ext(), delete=False)
        vk_temp.close()
        temp_files.append(vk_temp.name)
        fc = (
            f"[0:a]volume={original_volume:.4f}[main];"
            f"aevalsrc='{expr_l}|{expr_r}':c=stereo:s={sample_rate}:d={duration:.4f}[infra];"
            f"[main][infra]amix=inputs=2:duration=first:"
            f"dropout_transition=0:normalize=0,"
            f"alimiter=limit=0.98:attack=5:release=50[out]"
        )
        cmd_vk = [
            'ffmpeg', '-i', current_input,
            '-filter_complex', fc,
            '-map', '[out]',
        ] + self._temp_codec() + ['-y', vk_temp.name]
        success_vk, result_vk = self._safe_subprocess_run(cmd_vk, "vk infrasonic", allow_fail=False)
        if success_vk and self._verify_audio(vk_temp.name):
            return vk_temp.name
        err = self._extract_ffmpeg_error(result_vk.stderr) if result_vk else "неизвестная ошибка"
        self.on_error(f"VK Инфразвук: не удалось применить — {err}")
        return current_input

    def _generate_ultrasonic_track(self, current_input, temp_files):
        if not self.settings['methods'].get('ultrasonic_noise', False):
            return None
        ultrasonic_temp = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
        ultrasonic_temp.close()
        temp_files.append(ultrasonic_temp.name)
        freq = float(self.settings.get('ultrasonic_freq', 21000))
        level = self.settings.get('ultrasonic_level', 0.001)
        duration = self._get_duration(current_input)
        sr = self._get_sample_rate(current_input) or 44100
        # MP3 (LAME) режет всё выше ~18.5 кГц — маркер 21+ кГц не переживает кодирование.
        # Опускаем частоту в диапазон, который кодек ещё воспроизводит.
        max_freq = min(17000.0, sr / 2 - 500)
        if freq > max_freq:
            self.on_error(
                f"Ультразвук: {freq:.0f} Гц не выживает в MP3 (кодек режет выше ~18 кГц) — "
                f"частота снижена до {max_freq:.0f} Гц")
            freq = max_freq
        if freq <= 0 or duration <= 0:
            return None
        if duration > 0:
            ok_ultra, _ = self._safe_subprocess_run(
                ['ffmpeg', '-f', 'lavfi',
                 '-i', f'sine=frequency={freq:.1f}:sample_rate={sr}:duration={duration:.4f}',
                 '-af', f'volume={level}', '-y', ultrasonic_temp.name],
                "ultrasonic", allow_fail=True)
            if ok_ultra and os.path.exists(ultrasonic_temp.name) and os.path.getsize(ultrasonic_temp.name) > 0:
                return ultrasonic_temp.name
            if not ok_ultra:
                self.on_error(f"Ультразвук: не удалось сгенерировать сигнал {freq:.0f} Гц")
        return None

    def _compute_filters(self, current_input):
        filters = list(self._build_filters(current_input))
        if self.settings['methods'].get('fade_out', False):
            fade_dur = self.settings.get('fade_duration', 5)
            duration = self._get_duration(current_input)
            # atempo меняет таймлайн — старт фейда считаем по длительности после speed
            if self.settings['methods'].get('speed', False):
                spd = float(self.settings.get('speed_value', 1.0))
                if spd > 0:
                    duration = duration / spd
            if duration > 0:
                fade_start = max(0, duration - fade_dur)
                filters.append(f"afade=t=out:st={fade_start:.3f}:d={fade_dur}")
        vol = self.settings.get('output_volume', 1.0)
        if abs(vol - 1.0) > 0.001:
            filters.append(f"volume={vol:.4f}")
        # дописка тишины — строго последней, после фейда
        if self.settings['methods'].get('silence', False):
            filters.append(f"apad=pad_dur={self.settings['silence_duration']}")
        # fp_jitter меняет временну́ю базу, поэтому ставится самым последним:
        # тогда рассчитанные выше тайминги фейдов не разъезжаются
        if self.settings['methods'].get('fp_jitter', False):
            fpj = self._build_fp_jitter_filter(current_input)
            if fpj:
                filters.append(fpj)
        return ", ".join(filters) if filters else None

    def _extract_cover(self, track_info, temp_files):
        if self.settings.get('selected_cover_path') and os.path.exists(self.settings['selected_cover_path']):
            return self.settings['selected_cover_path']
        if self.settings.get('preserve_cover') and track_info and track_info.cover_data:
            cover_ext = track_info.cover_mime.split('/')[1] if '/' in track_info.cover_mime else 'jpg'
            cover_temp = tempfile.NamedTemporaryFile(suffix=f'.{cover_ext}', delete=False)
            cover_temp.write(track_info.cover_data)
            cover_temp.close()
            temp_files.append(cover_temp.name)
            return cover_temp.name
        return None

    def _resolve_tags(self, orig):
        _orig_title  = orig['title']
        _orig_artist = orig['artist']
        _orig_album  = orig['album']
        _orig_year   = orig['year']
        _orig_genre  = orig['genre']

        _meta_title = self.metadata.get('title', '')
        if _meta_title:
            if self.metadata.get('_append_title') and _orig_title:
                title_to_use = _orig_title + _meta_title
            else:
                title_to_use = _meta_title
        elif self.settings.get('rename_files', True):
            title_to_use = f"{random.choice(_RANDOM_TITLES)} {random.randint(1, 999)}"
        else:
            title_to_use = _orig_title if self.settings.get('preserve_metadata') else ""

        def _meta_tag(key, orig_val):
            v = self.metadata.get(key, '')
            if v and self.metadata.get(f'_append_{key}') and orig_val:
                return orig_val + v
            if v:
                return v
            return orig_val if self.settings.get('preserve_metadata') else ''

        return {
            'title':  title_to_use,
            'artist': _meta_tag('artist', _orig_artist),
            'album':  _meta_tag('album',  _orig_album),
            'year':   _meta_tag('year',   _orig_year),
            'genre':  _meta_tag('genre',  _orig_genre),
        }

    def _build_ffmpeg_cmd(self, current_input, ultrasonic_path, filters, cover_source_path, tags, output_file):
        cmd = ['ffmpeg', '-i', current_input]
        has_ultrasonic = bool(ultrasonic_path)

        if has_ultrasonic:
            cmd.extend(['-i', ultrasonic_path])
            cover_idx = 2
            amix = "[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0:normalize=0,alimiter=limit=0.98:attack=5:release=50"
            fc = f"{amix}[_mixed];[_mixed]{filters}[_out]" if filters else f"{amix}[_out]"
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

        if tags['title']:
            cmd.extend(['-metadata', f"title={tags['title']}"])
        if tags['artist']:
            cmd.extend(['-metadata', f"artist={tags['artist']}"])
        if tags['album']:
            cmd.extend(['-metadata', f"album={tags['album']}"])
        if tags['year']:
            cmd.extend(['-metadata', f"date={tags['year']}"])
        if tags['genre']:
            cmd.extend(['-metadata', f"genre={tags['genre']}"])

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
            if self.settings['methods'].get('broken_duration', False):
                # без Xing/Info поддельная длительность не применяется вообще:
                # плееры и VK считают время по кадрам. Приоритет у broken_duration.
                if not self._xing_conflict_warned:
                    self._xing_conflict_warned = True
                    self.on_error(
                        "«Удалить Xing/Info заголовок» проигнорирован: он отменяет "
                        "«Сломанную длительность» (без Xing подделка не применяется)")
            else:
                cmd.extend(['-write_xing', '0'])

        cmd.extend(['-id3v2_version', '3', '-y', output_file])
        return cmd

    def _apply_post_processing(self, output_file):
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
                self._apply_broken_duration(
                    output_file,
                    self.settings.get('broken_type', 0),
                )
            except Exception as e:
                self.on_error(f"Broken Duration: {e}")
        if self.settings['methods'].get('embed_text', False):
            embed_text = (self.settings.get('embed_text_value', '') or '').strip()
            if embed_text:
                try:
                    self._apply_embed_text(output_file, embed_text)
                except Exception as e:
                    self.on_error(f"Вшить текст: {e}")


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
                meta = self._read_track_metadata(file_path, track_info)
                output_file = self._resolve_output_path(
                    i, original_name,
                    meta['ex_title'], meta['ex_artist'], meta['ex_album'], meta['ex_year'])

                current_input = self._apply_cut_fragment(current_input, temp_files)
                current_input = self._apply_trim_silence(current_input, temp_files)
                current_input = self._apply_merge(current_input, temp_files)
                current_input = self._apply_overlay(current_input, temp_files)
                current_input = self._apply_insert_audio(current_input, temp_files)
                current_input = self._apply_vk_infrasonic(current_input, temp_files)
                ultrasonic_path = self._generate_ultrasonic_track(current_input, temp_files)
                self._run_temp_files = temp_files
                filters = self._compute_filters(current_input)
                cover_source_path = self._extract_cover(track_info, temp_files)
                tags = self._resolve_tags(meta['orig'])
                cmd = self._build_ffmpeg_cmd(
                    current_input, ultrasonic_path, filters, cover_source_path, tags, output_file)

                success, result = self._safe_subprocess_run(cmd, "main processing", timeout=1800)

                if success and os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    expected_duration = self._get_duration(current_input)
                    actual_duration = self._get_duration(output_file)
                    ratio = 0.9
                    if self.settings['methods'].get('speed', False):
                        spd = self.settings.get('speed_value', 1.0)
                        if spd > 0:
                            ratio = 0.9 / spd
                    # pitch теперь компенсируется через atempo — длительность не меняется
                    if self.settings['methods'].get('cut_fragment', False):
                        cut_dur = self.settings.get('cut_duration', 2)
                        if expected_duration > 0:
                            ratio = ratio * (1 - cut_dur / expected_duration)
                    if expected_duration > 1 and actual_duration < expected_duration * ratio:
                        success = False
                        self.on_error(
                            f"{os.path.basename(file_path)}: результат обрезан "
                            f"({actual_duration:.1f} сек вместо {expected_duration:.1f} сек) — "
                            f"похоже, исходный файл повреждён"
                        )

                if success and os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    self._apply_post_processing(output_file)

                for temp_file in temp_files:
                    try:
                        if os.path.exists(temp_file):
                            os.unlink(temp_file)
                    except Exception:
                        pass

                if success and os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    success_count += 1
                    self.on_file_complete(file_path, True, output_file)
                    if self.settings.get('delete_original', False):
                        try:
                            os.unlink(file_path)
                        except Exception:
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
                    except Exception:
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

    def _apply_embed_text(self, file_path, text):
        audio = MP3(file_path)
        if audio.tags is None:
            audio.add_tags()
        audio.tags['USLT::XXX'] = USLT(encoding=3, lang='XXX', desc='', text=text)
        audio.save(v2_version=3)

    def _reorder_id3_tags(self, file_path):
        audio = MP3(file_path)
        if audio.tags:
            audio.tags.update_to_v23()
            audio.save()

    @staticmethod
    def _find_first_audio_frame(data):
        """Оффсет первого аудиофрейма: сразу за ID3-тегом.

        Если размер тега расходится с реальным sync-словом, доискиваемся его
        рядом — иначе Xing/Info не находится и длительность не подделывается.
        """
        frame_start = 0
        if bytes(data[:3]) == b'ID3' and len(data) > 10:
            tag_size = ((data[6] & 0x7F) << 21) | ((data[7] & 0x7F) << 14) | \
                       ((data[8] & 0x7F) << 7) | (data[9] & 0x7F)
            frame_start = min(10 + tag_size, max(0, len(data) - 600))

        def is_sync(pos):
            return (pos + 1 < len(data)
                    and data[pos] == 0xFF and (data[pos + 1] & 0xE0) == 0xE0)

        if not is_sync(frame_start):
            limit = min(len(data) - 1, frame_start + 8192)
            pos = frame_start
            while pos < limit:
                if is_sync(pos):
                    return pos
                pos += 1
        return frame_start

    def _apply_broken_duration(self, file_path, bug_type):
        audio = MP3(file_path)
        if audio.tags is None:
            audio.add_tags()

        _24H_MS = 86_400_000

        fake_sec = _fake_duration_sec(
            bug_type, self.settings.get('broken_custom_sec', _MIN_FAKE_SEC))
        fake_ms = math.ceil(fake_sec * 1000)
        if bug_type == 6:
            # переполнение: качаем значение вокруг границы суток на ±1 мс
            fake_ms = random.choice([_24H_MS, _24H_MS - 1, _24H_MS + 1])

        audio.tags['TLEN'] = TLEN(encoding=3, text=str(fake_ms))

        if self.settings.get('broken_multi_tlen', False):
            # конфликтующие значения держим в тех же границах, что и основное, —
            # иначе плеер может вытащить отсюда длительность меньше часа
            lo = ((_MIN_FAKE_SEC + _FLOOR_GUARD_SEC) * 1000
                  if bug_type in _BIG_FAKE_TYPES else 100)
            conflicting = [
                random.randint(lo, max(lo, min(fake_ms * 3, _24H_MS))),
                random.randint(lo, max(lo, _24H_MS // 2)),
                random.randint(min(max(fake_ms, lo), _24H_MS), _24H_MS),
            ]
            for i, val in enumerate(conflicting):
                audio.tags[f'TXXX:DURATION_{i}'] = TXXX(
                    encoding=3, desc=f'DURATION_{i}', text=str(val))
            audio.tags['TXXX:LENGTH'] = TXXX(
                encoding=3, desc='LENGTH', text=str(random.randint(lo, _24H_MS)))
            dur_bytes = random.randint(lo, _24H_MS).to_bytes(4, 'big')
            audio.tags['PRIV:com.apple.streaming.transportStreamTimestamp'] = PRIV(
                owner='com.apple.streaming.transportStreamTimestamp', data=dur_bytes)
            audio.tags['PRIV:duration_ms'] = PRIV(
                owner='duration_ms',
                data=random.randint(lo, _24H_MS).to_bytes(4, 'big'))

        audio.save(v2_version=3)

        with open(file_path, 'rb') as f:
            data = bytearray(f.read())

        modified = False
        corrupt_toc = self.settings.get('broken_corrupt_toc', False)
        corrupt_lame = self.settings.get('broken_corrupt_lame', False)
        corrupt_vbri = self.settings.get('broken_corrupt_vbri', False)

        # Xing/Info/VBRI живут только в первом аудиофрейме сразу после ID3-тега.
        # Поиск по всему файлу может найти эти байты внутри аудиоданных и испортить их.
        frame_start = self._find_first_audio_frame(data)
        search_end = min(len(data) - 120, frame_start + 400)

        header_sr, samples_per_frame = _mp3_frame_geometry(data, frame_start)
        # округление вверх: иначе ровно час кадров превращается в 3599.97 с
        max_frames = math.ceil(_MAX_FAKE_SEC * header_sr / samples_per_frame)
        fake_frames = math.ceil(fake_sec * header_sr / samples_per_frame)
        fake_frames = min(max(fake_frames, 0), max_frames)

        vbr_pos = data.find(b'Xing', frame_start, search_end)
        if vbr_pos == -1:
            vbr_pos = data.find(b'Info', frame_start, search_end)

        if vbr_pos >= 0:
            flags = int.from_bytes(data[vbr_pos + 4: vbr_pos + 8], 'big')

            real_frames = 0
            real_bytes = 0
            frames_offset = bytes_offset = toc_offset = -1
            cursor = vbr_pos + 8

            if flags & 0x01:
                frames_offset = cursor
                real_frames = int.from_bytes(data[cursor: cursor + 4], 'big')
                cursor += 4
            if flags & 0x02:
                bytes_offset = cursor
                real_bytes = int.from_bytes(data[cursor: cursor + 4], 'big')
                cursor += 4
            if flags & 0x04:
                toc_offset = cursor

            if frames_offset >= 0:
                data[frames_offset: frames_offset + 4] = fake_frames.to_bytes(4, 'big')
                modified = True

            if bytes_offset >= 0:
                # bytes держим согласованным с frames через реальный средний
                # размер кадра: случайное значение уводит часть плееров
                # (и транскодер VK) на совершенно другую длительность.
                # Но ниже размера аудиопотока опускаться нельзя: ffmpeg
                # отбрасывает Xing целиком, если header_filesize заметно
                # меньше файла, и тогда длительность не подделывается вовсе
                # (случай длинного исходника и более короткой подделки).
                if fake_frames == 0:
                    fake_bytes_val = 0
                else:
                    if real_frames > 0 and real_bytes > 0:
                        fake_bytes_val = int(fake_frames * real_bytes / real_frames)
                    else:
                        fake_bytes_val = real_bytes
                    fake_bytes_val = max(fake_bytes_val,
                                         max(len(data) - frame_start, 0) + 1024)
                fake_bytes_val = min(max(fake_bytes_val, 0), 0xFFFFFFFE)
                data[bytes_offset: bytes_offset + 4] = fake_bytes_val.to_bytes(4, 'big')
                modified = True

            if corrupt_toc and toc_offset >= 0 and toc_offset + 100 <= len(data):
                toc = list(data[toc_offset: toc_offset + 100])
                random.shuffle(toc)
                for _ in range(20):
                    toc[random.randint(0, 99)] = random.choice([0, 255, random.randint(0, 255)])
                data[toc_offset: toc_offset + 100] = bytes(toc)
                modified = True

            if corrupt_lame:
                lame_pos = -1
                for tag in (b'LAME', b'Lavc', b'lavf'):
                    lame_pos = data.find(tag, vbr_pos, vbr_pos + 200)
                    if lame_pos > 0:
                        break
                if lame_pos > 0 and lame_pos + 36 <= len(data):
                    delay_off = lame_pos + 21
                    if delay_off + 3 <= len(data):
                        fake_delay = random.randint(0, 4095)
                        fake_pad = random.randint(0, 4095)
                        data[delay_off] = (fake_delay >> 4) & 0xFF
                        data[delay_off + 1] = ((fake_delay & 0x0F) << 4) | ((fake_pad >> 8) & 0x0F)
                        data[delay_off + 2] = fake_pad & 0xFF
                        modified = True

        if corrupt_vbri:
            vbri_pos = data.find(b'VBRI', frame_start, min(len(data) - 26, frame_start + 400))
            if vbri_pos >= 0:
                real_vbri_bytes = int.from_bytes(data[vbri_pos + 10: vbri_pos + 14], 'big')
                real_vbri_frames = int.from_bytes(data[vbri_pos + 14: vbri_pos + 18], 'big')
                if fake_frames == 0:
                    fake_vbri_bytes = 0
                else:
                    if real_vbri_frames > 0 and real_vbri_bytes > 0:
                        fake_vbri_bytes = int(fake_frames * real_vbri_bytes / real_vbri_frames)
                    else:
                        fake_vbri_bytes = real_vbri_bytes
                    fake_vbri_bytes = max(fake_vbri_bytes,
                                          max(len(data) - frame_start, 0) + 1024)
                fake_vbri_bytes = min(max(fake_vbri_bytes, 0), 0xFFFFFFFE)
                data[vbri_pos + 10: vbri_pos + 14] = fake_vbri_bytes.to_bytes(4, 'big')
                data[vbri_pos + 14: vbri_pos + 18] = fake_frames.to_bytes(4, 'big')
                modified = True

        if not modified:
            self.on_error(
                f"{os.path.basename(file_path)}: Xing/Info/VBRI заголовок не найден — "
                f"поддельная длительность попала только в TLEN, "
                f"большинство плееров её игнорирует")

        if modified:
            out_tmp = file_path + '.vkmod_out'
            with open(out_tmp, 'wb') as f:
                f.write(data)
            os.replace(out_tmp, file_path)
