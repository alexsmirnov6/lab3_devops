"""Build the course report from the prepared dataset and recorded results."""

import argparse
import json
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from pypdf import PdfReader
from reportlab.platypus import (
    Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
)

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence/rustfs-validation"
PREPARED = ROOT / "data/prepared"
RESULTS = ROOT / "results/rustfs"
ASSETS = ROOT / "report/figures"
BLACK = colors.black
INK = colors.black
RULE = colors.HexColor("#777777")


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def spaced(value):
    return f"{value:,.0f}".replace(",", " ")


def money(value):
    return f"{value:,.0f}".replace(",", " ")


def make_report(font_dir: Path, destination: Path) -> None:
    pdfmetrics.registerFont(TTFont("Report", str(font_dir / "arial.ttf")))
    pdfmetrics.registerFont(TTFont("Report-Bold", str(font_dir / "arialbd.ttf")))
    pdfmetrics.registerFont(TTFont("Report-Italic", str(font_dir / "ariali.ttf")))
    base = {
        "title": ParagraphStyle("Title2", fontName="Report-Bold", fontSize=22, leading=28,
                                textColor=BLACK, spaceAfter=13),
        "subtitle": ParagraphStyle("Subtitle2", fontName="Report", fontSize=13, leading=18,
                                    textColor=BLACK, spaceAfter=8),
        "h1": ParagraphStyle("H1-2", fontName="Report-Bold", fontSize=19, leading=24,
                              textColor=BLACK, spaceAfter=10),
        "h2": ParagraphStyle("H2-2", fontName="Report-Bold", fontSize=12, leading=16,
                              textColor=BLACK, spaceBefore=7, spaceAfter=5),
        "body": ParagraphStyle("Body2", fontName="Report", fontSize=9.6, leading=14,
                                textColor=INK, spaceAfter=7),
        "small": ParagraphStyle("Small2", fontName="Report", fontSize=8, leading=10.5,
                                textColor=INK, spaceAfter=4),
        "cell": ParagraphStyle("Cell2", fontName="Report", fontSize=7.8, leading=10,
                               textColor=INK),
        "cover": ParagraphStyle("Cover2", fontName="Report", fontSize=10, leading=15,
                                textColor=INK, alignment=TA_CENTER),
    }
    def para(value, style="body"):
        value = str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return Paragraph(value.replace("\n", "<br/>"), base[style])

    def table(headers, rows, widths, font_size=7.8):
        cell = ParagraphStyle(f"table-{font_size}", fontName="Report", fontSize=font_size,
                              leading=font_size + 2, textColor=INK)
        head = ParagraphStyle(f"head-{font_size}", parent=cell, fontName="Report-Bold",
                              textColor=BLACK)
        def as_cell(value, style):
            if isinstance(value, Paragraph):
                return value
            text = str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            return Paragraph(text, style)
        data = [[as_cell(v, head) for v in headers]]
        data += [[as_cell(v, cell) for v in row] for row in rows]
        result = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
        result.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LINEBELOW", (0, 0), (-1, 0), .8, BLACK),
            ("LINEBELOW", (0, 1), (-1, -1), .3, RULE),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        return result

    metadata = read_json(PREPARED / "dataset_metadata.json")
    source_check = read_json(EVIDENCE / "variant_check.json")
    mr = read_json(EVIDENCE / "mapreduce_run.json")
    duck = read_json(EVIDENCE / "duckdb_run.json")
    verified = read_json(EVIDENCE / "verification.json")
    published = read_json(EVIDENCE / "published_results.json")
    inventory = read_json(EVIDENCE / "list_objects.json")
    profile = read_json(EVIDENCE / "profiling.json")
    failure = read_json(EVIDENCE / "mapper_failure.json")
    recovered = read_json(EVIDENCE / "recovery_verification.json")
    regions = read_json(EVIDENCE / "regions.json")
    salary = read_json(EVIDENCE / "salary_effects.json")
    skills = read_json(RESULTS / "duckdb/skills.json")

    if not all((source_check["exact_id_set_match"], verified["all_equal"], published["all_verified"],
                profile["verification"]["all_equal"], recovered["all_equal"], failure["retries"] == 1)):
        raise ValueError("A required result check did not pass")
    if min(item["attempts"] for item in profile["methods"].values()) < 3:
        raise ValueError("Three measurements are required for each method")
    if {item["key"] for item in inventory if item["key"].startswith("results/mapreduce/")} != {
        "results/mapreduce/skills.json", "results/mapreduce/pairs.json", "results/mapreduce/triples.json"
    }:
        raise ValueError("The S3 listing does not contain all three MapReduce results")

    width = A4[0] - 36 * mm
    story = []
    def heading(title):
        story.append(para(title, "h1"))
    def image(name, width_points, height_points):
        asset = ASSETS / name
        im = Image(str(asset), width=width_points, height=height_points, kind="proportional")
        return im
    def figure(name, caption, w, h):
        story.extend([image(name, w, h), para(caption, "small"), Spacer(1, 5)])
    def page():
        story.append(PageBreak())
    # Титульная страница и краткое описание задачи.
    story += [Spacer(1, 27 * mm), para("ЛАБОРАТОРНАЯ РАБОТА 3", "subtitle"),
              para("Распределённый анализ\nвакансий DevOps", "title"),
              para("RustFS, MapReduce и аналитика DuckDB", "subtitle"), Spacer(1, 7 * mm),
              para("Вариант 11 | Смирнов Александр | J3310", "cover"),
              para("18 октября 2026 года", "cover"), Spacer(1, 18 * mm),
              para("Цель работы", "h2"),
              para("Подготовить сегмент вакансий DevOps, хранить данные в объектном "
                   "S3-совместимом хранилище, сравнить собственный MapReduce с SQL в DuckDB "
                   "и исследовать навыки, зарплаты и региональные особенности."),
              para("Результат", "h2"),
              para("Из 229 388 вакансий выбрано 6 215. Для них рассчитаны частоты 1 541 навыка, "
                   "25 462 пар и 195 366 троек. Вычисления MapReduce совпали с независимым "
                   "запросом DuckDB по каждому ключу и каждой частоте."), Spacer(1, 18 * mm),
              table(["Основной этап", "Полученный результат"], [
                  ["Подготовка данных", "6 215 записей NDJSON; контрольный SHA-256"],
                  ["Хранилище", "RustFS, бакет hh-lake; результаты доступны по S3 API"],
                  ["Подсчёт", "MapReduce с combiner и независимый DuckDB httpfs"],
                  ["Проверка", "Полное совпадение трёх JSON; 3 запуска каждого метода"],
                  ["Аналитика", "Зарплатные сочетания и три региона с наибольшим числом вакансий"],
              ], [110, width - 110])]
    page()

    # Данные и постановка отбора.
    heading("Данные и формирование сегмента")
    story += [para("Исходный файл - CSV с вакансиями IT-специалистов. Отбор выполнялся "
                   "по названию позиции. В сегмент включены DevOps и DevSecOps, русское "
                   "написание «девопс», SRE, Site Reliability и Platform Engineer. Один "
                   "SQL-запрос к исходному CSV проверяет тот же критерий независимо от Python."),
              table(["Показатель", "Значение"], [
                  ["Исходный файл", metadata["source_file"]],
                  ["Размер", f"{spaced(metadata['source_bytes'])} байт"],
                  ["SHA-256", metadata["source_sha256"]],
                  ["Записей в исходном CSV", spaced(metadata["source_rows"])],
                  ["Выбрано для анализа", spaced(metadata["selected_rows"])],
                  ["Из выбранных: с навыками", f"{spaced(metadata['selected_with_skills'])} "
                   f"({metadata['selected_with_skills'] / metadata['selected_rows']:.1%})"],
                  ["Сопоставимая зарплата в RUR", f"{spaced(metadata['selected_with_salary_rur'])} "
                   f"({metadata['selected_with_salary_rur'] / metadata['selected_rows']:.1%})"],
                  ["Дата публикации", f"{metadata['published_at_min']} - {metadata['published_at_max']}"],
              ], [165, width - 165]), Spacer(1, 8),
              para(f"Независимый SQL отобрал {spaced(source_check['duckdb_selected'])} строк; "
                   f"SQL-only = {source_check['sql_only']}, prepared-only = {source_check['prepared_only']}. "
                   f"SHA-256 обоих отсортированных наборов ID совпал: {source_check['sql_ids_sha256']}.", "small"),
              para("Подготовка", "h2"),
              para("Каждая строка NDJSON соответствует одной вакансии. Навыки приведены к "
                   "нижнему регистру, пробелы нормализованы, повторы удалены. Сохраняются "
                   "идентификатор, дата, регион, исходные границы зарплаты и оценка зарплаты "
                   "gross в рублях. Файл сегмента имеет размер "
                   f"{spaced(metadata['variant_bytes'])} байт; SHA-256: {metadata['variant_sha256']}.")]
    figure("popular-skills.png", "Рисунок 1. Наиболее частые навыки в отобранных вакансиях.", width * .86, 105 * mm)
    page()

    # Схема и реальные доказательства S3.
    heading("Объектное хранилище")
    story += [para("RustFS запущен как локальный S3-совместимый сервис. В бакете hh-lake "
                   "размещены исходный CSV, подготовленный NDJSON, журналы и результаты. "
                   "Для всех трёх выходных JSON выполнена повторная загрузка из бакета; "
                   "проверены количество байт и SHA-256 фактически прочитанного содержимого."),
              table(["Ключ результата", "Байт", "SHA-256 скачанного объекта"],
                    [[obj["key"].removeprefix("results/mapreduce/"), spaced(obj["bytes"]),
                      obj["sha256_downloaded"]] for obj in published["objects"]],
                    [120, 90, width - 210], font_size=7.2), Spacer(1, 5),
              para("S3 ETag не используется как SHA-256: для multipart-объектов он имеет "
                   "другой формат. Вместо этого сравниваются метаданные загрузки и хеш "
                   "потока, скачанного обратно из S3."),
              para("Интерфейс RustFS", "h2")]
    figure("rustfs-buckets.png", "Рисунок 2. Бакет hh-lake в консоли RustFS.", width * .65, 75 * mm)
    page()
    heading("Содержимое бакета")
    story += [para("Пространство имён разделяет исходные и производные данные. Такой порядок "
                   "помогает повторно использовать исходный файл, не смешивая его с "
                   "результатами вычислений."),
              table(["Префикс", "Содержимое"], [
                  ["raw/", "Исходный CSV"],
                  ["variants/devops/", "NDJSON-сегмент и метаданные подготовки"],
                  ["results/mapreduce/", "Три результата навыков, пар и троек"],
                  ["results/duckdb/", "Независимые SQL-результаты, зарплаты и регионы"],
                  ["evidence/", "Инвентаризация и журналы проверок"],
                  ["supplementary/", "Выборка JSON-вакансий"],
              ], [155, width - 155]), Spacer(1, 10)]
    figure("rustfs-prefixes.png", "Рисунок 3. Префиксы бакета hh-lake в консоли RustFS.", width * .58, 119 * mm)
    page()

    # Алгоритм.
    heading("Обработка чанков и подсчёт")
    story += [para("Размер S3-объекта получают через HEAD. Файл делится на непрерывные "
                   "байтовые диапазоны. Mapper читает каждый диапазон запросом Range GET."),
              table(["Ситуация на границе", "Действие Mapper"], [
                  ["Диапазон начинается внутри строки", "Пропустить байты до следующего перевода строки"],
                  ["Диапазон заканчивается внутри строки", "Дочитать запись блоками по 64 КиБ"],
                  ["Строка длиннее 64 КиБ", "Продолжать дочитывание до конца записи"],
                  ["Последняя строка без \\n", "Обработать оставшиеся байты объекта"],
                  ["Ошибка JSON или структура навыков нарушена", "Завершить с номером чанка и причиной"],
              ], [205, width - 205]), Spacer(1, 8),
              para("Чанк, содержащий первый байт записи, отвечает за всю эту запись. "
                   "Поэтому дочитанные соседние байты не приводят к двойному учёту. "
                   "В памяти комбинации представлены кортежами навыков; отдельный символ-разделитель "
                   "не нужен. В итоговом JSON пары и тройки сериализуются как списки."),
              para("В baseline Mapper передаёт все комбинации. В режиме с combiner он складывает "
                   "одинаковые комбинации в локальный Counter и передаёт счётчики Reducer. "
                   "Reducer объединяет оба формата одинаковым вызовом update()."),
              para("Восстановление", "h2"),
              para(f"В тестовом прогоне намеренно отказал второй Mapper. Координатор повторил "
                   f"один чанк: retries = {failure['retries']}. После повтора обработаны "
                   f"{spaced(failure['vacancies'])} вакансий; все три результата совпали с обычным запуском. "
                   "Повторяются временные сетевые ошибки и коды S3/HTTP, обозначающие временный сбой. "
                   "Ошибки данных повторно не выполняются.")]
    page()

    # DuckDB, сохранение, верификация.
    heading("Независимый подсчёт и проверка")
    story += [para("DuckDB читает тот же NDJSON из RustFS через расширение httpfs. "
                   "Массив навыков разворачивается в строки; для пар и троек используются "
                   "самосоединения по vacancy_id и лексикографическое условие skill1 < skill2 < skill3. "
                   "Таким образом, Python MapReduce и SQL получают комбинации независимо."),
              table(["Стек", "Уникальные ключи", "Сумма частот", "Результат"], [
                  ["Навыки", spaced(1541), spaced(35158), "совпало"],
                  ["Пары", spaced(25462), spaced(173204), "совпало"],
                  ["Тройки", spaced(195366), spaced(687102), "совпало"],
              ], [90, 125, 125, 90]), Spacer(1, 8),
              para("Сопоставлены полные JSON-документы: метаданные, порядок ключей и частота "
                   "каждой комбинации. SHA-256 канонических результатов:", "body"),
              table(["Результат", "SHA-256"], [[item["stack"], item["sha256_mapreduce"]]
                  for item in verified["checks"]], [100, width - 100], font_size=7.4), Spacer(1, 7),
              para(f"S3 API перечислил {len(inventory)} объекта в префиксе results/mapreduce/. "
                   "Там находятся skills.json, pairs.json и triples.json. Результаты не ограничены "
                   "первой строкой или top-N: полные частотные таблицы сохранены в хранилище."),
              para("Результаты MapReduce в консоли RustFS", "h2")]
    figure("mapreduce-results.jpg", "Рисунок 4. Объекты skills.json, pairs.json и triples.json.", width * .32, 104 * mm)
    page()

    # Частоты и производительность.
    heading("Частота навыков и сочетаний")
    combo_rows = []
    for label, unique, emissions, formula in [
        ("1 навык", 1541, 35158, "k"),
        ("2 навыка", 25462, 173204, "k(k - 1) / 2"),
        ("3 навыка", 195366, 687102, "k(k - 1)(k - 2) / 6"),
    ]:
        combo_rows.append([label, spaced(unique), spaced(emissions), formula])
    story += [para("Число уникальных комбинаций растёт с размером стека, потому что "
                   "каждая вакансия создаёт сочетания навыков. Для k уникальных навыков "
                   "количество одиночных навыков равно k, пар - C(k,2), троек - C(k,3). "
                   "При десяти навыках вакансия порождает 10 навыков, 45 пар и 120 троек."),
              table(["Размер стека", "Уникальные", "Всего в вакансиях", "На одну вакансию"],
                    combo_rows, [100, 100, 130, width - 330]), Spacer(1, 10),
              para("«Уникальные» считает разные ключи во всём сегменте. «Всего» суммирует "
                   "вхождения ключа по вакансиям. Одна и та же пара, найденная в тысяче вакансий, "
                   "вносит одну единицу в число уникальных пар и тысячу в общую частоту."),
              para("Наиболее частые навыки", "h2")]
    figure("popular-skills.png", "Рисунок 5. Десять самых частых навыков; частота указана в вакансиях.", width * .83, 100 * mm)
    page()
    heading("Сравнение времени и памяти")
    methods = profile["methods"]
    labels = {"mapreduce_baseline": "MapReduce baseline", "mapreduce_optimized": "MapReduce + combiner",
              "duckdb_sql": "DuckDB через httpfs"}
    measurements = []
    for key, method in methods.items():
        for i, run in enumerate(method["runs"], 1):
            measurements.append([labels[key], i, f"{run['wall_seconds']:.3f}", f"{run['peak_rss_mib']:.1f}"])
    story += [para(f"Каждый метод запускался трижды на одном объекте RustFS, с четырьмя "
                   f"процессами MapReduce или четырьмя потоками DuckDB и чанком "
                   f"{spaced(profile['chunk_bytes'])} байт. Время включает запуск процесса, "
                   "S3-чтение и запись результатов. В таблице приведены отдельные попытки."),
              table(["Метод", "Запуск", "Wall time, с", "Пик RSS, МиБ"], measurements,
                    [230, 50, 100, width - 380]), Spacer(1, 10),
              table(["Метод", "Медиана времени, с", "Медиана RSS, МиБ"], [
                  [labels[k], f"{v['wall_seconds_median']:.3f}", f"{v['peak_rss_mib_median']:.1f}"]
                  for k, v in methods.items()], [250, 125, width - 375]), Spacer(1, 10)]
    baseline = methods["mapreduce_baseline"]["runs"][0]
    optimized = methods["mapreduce_optimized"]["runs"][0]
    shrink = (1 - optimized["intermediate_pickle_bytes"] / baseline["intermediate_pickle_bytes"]) * 100
    story += [para(f"Combiner сократил число передаваемых элементов с "
                   f"{spaced(baseline['intermediate_items'])} до {spaced(optimized['intermediate_items'])}. "
                   f"Размер сериализованных payload уменьшился с "
                   f"{spaced(baseline['intermediate_pickle_bytes'])} до "
                   f"{spaced(optimized['intermediate_pickle_bytes'])} байт ({shrink:.1f}%). "
                   f"Медиана времени MapReduce изменилась с "
                   f"{methods['mapreduce_baseline']['wall_seconds_median']:.3f} до "
                   f"{methods['mapreduce_optimized']['wall_seconds_median']:.3f} с; "
                   f"пик RSS - с {methods['mapreduce_baseline']['peak_rss_mib_median']:.1f} "
                   f"до {methods['mapreduce_optimized']['peak_rss_mib_median']:.1f} МиБ."),
              para(f"В этом запуске DuckDB завершил расчёт за "
                   f"{methods['duckdb_sql']['wall_seconds_median']:.3f} с при пике "
                   f"{methods['duckdb_sql']['peak_rss_mib_median']:.1f} МиБ. Такой результат "
                   "объясним эффективным векторизованным выполнением SQL на небольшом наборе. "
                   "Профилирование характеризует этот стенд: RSS снимался каждые 10 мс, "
                   "а общие страницы процессов могли учитываться повторно."),
              para("Запуски выполнялись последовательно без очистки системного кеша. "
                   "Числа полезны для сравнения двух реализаций в указанных условиях; "
                   "они не предсказывают скорость другого компьютера или удалённого кластера.", "small")]
    page()

    # Зарплатный анализ.
    heading("Навыки и медианная зарплата")
    overall = salary["overall"]
    story += [para(f"Для {spaced(overall['n'])} вакансий с известной месячной зарплатой "
                   f"в RUR общая медиана gross равна {money(overall['median_rur_gross'])} руб. "
                   "При двух границах зарплатного диапазона используется середина, при одной - "
                   "указанная граница. Значения net переводятся в gross делением на 0,87. "
                   "Иностранные валюты исключаются."),
              para("Для пары базовым считается более востребованный одиночный навык. "
                   "Для тройки выбирается наиболее востребованная входящая пара. Разница "
                   "медиан равна медиане комбинации минус медиана базы. В публикацию включены "
                   f"пары минимум с {salary['minimum_salary_observations']['pairs']} зарплатными "
                   f"наблюдениями и тройки минимум с {salary['minimum_salary_observations']['triples']}.")]
    for key, title in (("pair_effects", "Пары с наибольшей разницей"),
                       ("triple_effects", "Тройки с наибольшей разницей")):
        rows = []
        records = salary[key]
        positive = sorted((r for r in records if r["delta_rur"] > 0), key=lambda r: -r["delta_rur"])[:3]
        negative = sorted((r for r in records if r["delta_rur"] < 0), key=lambda r: r["delta_rur"])[:3]
        for r in positive + negative:
            combo = " + ".join(r.get(f"skill{i}", "") for i in (1, 2, 3) if r.get(f"skill{i}"))
            rows.append([combo, r.get("base_skill", r.get("base_pair")), str(r["salary_n"]),
                         money(r["base_median"]), money(r["combo_median"]),
                         ("+" if r["delta_rur"] > 0 else "") + money(r["delta_rur"])])
        story += [para(title, "h2"), table(["Сочетание", "База", "n", "Медиана базы", "Медиана сочетания", "Разница, руб."],
                     rows, [130, 95, 30, 85, 95, width - 435], font_size=7.1), Spacer(1, 8)]
    story += [para("Положительная разница медиан показывает связь между сочетанием и "
                   "более высокой наблюдаемой зарплатой. Она не доказывает, что добавление "
                   "навыка повышает оплату: роли, стаж, отрасль и работодатель влияют на зарплату, "
                   "но здесь отдельно не контролируются. Наиболее отрицательные сочетания "
                   "также приведены, чтобы показать обе стороны распределения."),
              para("Навыки записаны работодателями в свободной форме, раскрытие оплаты "
                   "неполное. Поэтому медианы описывают вакансии с опубликованными суммами, "
                   "а не всех специалистов или весь рынок.", "small")]
    page()

    # Отдельная страница на регион с методикой, размером выборки, парами и тройками.
    insights = {
        "Москва": "Пара Docker + Kubernetes указана в 860 вакансиях; тройка с Linux - в 558. "
                  "У Москвы больше наблюдений и зарплатных раскрытий, чем у остальных выбранных регионов. "
                  "Высокая медиана может быть связана с составом работодателей и ролей; причинные факторы "
                  "этим сравнением не проверяются.",
        "Санкт-Петербург": "Самая частая пара - Docker + Linux (223 вакансии); далее Docker + Kubernetes (199). "
                           "Ansible встречается в четырёх из пяти самых частых троек. Доля раскрытых зарплат "
                           "выше московской, поэтому медианы основаны на разных частях выборки.",
        "Екатеринбург": "Две ведущие пары встречаются одинаково часто: Docker + Kubernetes и Kubernetes + Linux, "
                       "по 71 вакансии каждая. Основная тройка Docker + Kubernetes + Linux есть в 53 вакансиях "
                       "с навыками. Высокая доля сочетается с небольшой абсолютной выборкой и 43 зарплатными "
                       "наблюдениями; эту медиану следует интерпретировать осторожно.",
    }
    for region in regions["summary"]:
        name = region["region"]
        heading(name)
        story += [para(f"В выборке {spaced(region['vacancies'])} вакансий; навыки указаны в "
                       f"{spaced(region['vacancies_with_skills'])}. Известная зарплата есть у "
                       f"{region['salary_n']}. Медиана gross - {money(region['median_salary_rur_gross'])} руб.; "
                       f"в среднем {region['avg_skills_per_vacancy']:.2f} навыка на вакансию."),
                  para("В каждой строке ниже частота - число вакансий, где указана вся комбинация. "
                       "Доля с навыками рассчитана от вакансий с непустым списком навыков; "
                       "доля от всех вакансий использует полный размер сегмента региона.", "small")]
        for key, title in (("top_pairs", "Пять наиболее частых пар"),
                           ("top_triples", "Пять наиболее частых троек")):
            selected = [row for row in regions[key] if row["region"] == name]
            story += [para(title, "h2"),
                      table(["Сочетание", "Вакансий", "Доля с навыками, %", "Доля всех, %"],
                            [[" + ".join(row["skills"]), str(row["vacancies"]),
                              f"{row['share_pct']:.2f}", f"{row['share_all_vacancies_pct']:.2f}"]
                             for row in selected], [200, 65, 120, width - 385]), Spacer(1, 7)]
        story += [para("Что видно в выборке", "h2"), para(insights.get(name, "")),
                  para("Популярность описывает опубликованные требования. Она не показывает "
                       "глубину навыка, качество рабочего места или причинный эффект на зарплату.", "small")]
        page()

    # Итог и ссылки завершают основной рассказ.
    heading("Выводы и ограничения")
    story += [para("MapReduce по S3 Range GET обработал данные по чанкам; combiner сократил "
                   "передаваемые повторяющиеся комбинации. Независимые SQL-запросы DuckDB "
                   "к тому же NDJSON дали те же частоты навыков, пар и троек. S3 API подтвердил "
                   "наличие результатов в hh-lake и целостность скачанного содержимого."),
              para("На этом объёме DuckDB был быстрее и использовал меньший пик RSS. "
                   "Для лабораторной ценность MapReduce состоит в демонстрации распределения "
                   "задач и combiner; производительность зависит от размера данных, числа чанков, "
                   "запросов к S3 и сериализации."),
              para("В трёх регионах повторяются Linux, Docker и Kubernetes. Москва имеет больше "
                   "всего вакансий и зарплатных наблюдений; в Екатеринбурге абсолютные частоты "
                   "меньше, хотя доля ведущей тройки среди вакансий с навыками выше. "
                   "Неполное раскрытие зарплат и малый региональный размер не позволяют "
                   "трактовать медианы как гарантированные рыночные уровни."),
              para("Ограничения", "h2"),
              table(["Фактор", "Как учитывать при интерпретации"], [
                  ["Оплата раскрыта не для всех вакансий", "Зарплатная выборка не является случайной"],
                  ["Свободный текст названий и навыков", "Фильтр может пропускать и включать пограничные случаи"],
                  ["Диапазон и net-to-gross 13%", "Оценка сопоставимости, а не расчёт зарплаты сотрудника"],
                  ["Медианная разница", "Ассоциация; стаж, роль и работодатель не контролируются"],
                  ["Региональный состав", "Частоты и медианы зависят от размера и структуры выборки"],
                  ["Память процесса", "Опрос раз в 10 мс; общие страницы могут учитываться повторно"],
              ], [165, width - 165]),
              para("Воспроизведение", "h2"),
              para("Подготовить CSV и RustFS согласно README. Выполнить "
                   "scripts/validate_rustfs.ps1 для загрузки входных данных, трёх расчётов, "
                   "проверки S3, восстановления Mapper и профилирования. JSON-результаты "
                   "и параметры запуска сохраняются в evidence/rustfs-validation/.", "small")]
    page()
    heading("Источники")
    sources = [
        ("Исходный CSV", f"{metadata['source_file']}; SHA-256 {metadata['source_sha256']}"),
        ("Подготовленный NDJSON", f"{metadata['variant_object']}; SHA-256 {metadata['variant_sha256']}"),
        ("Документация RustFS", "https://docs.rustfs.com/"),
        ("DuckDB httpfs: S3 API", "https://duckdb.org/docs/stable/core_extensions/httpfs/s3api"),
        ("DuckDB: чтение JSON", "https://duckdb.org/docs/stable/data/json/loading_json"),
        ("Python: itertools.combinations", "https://docs.python.org/3/library/itertools.html#itertools.combinations"),
        ("S3 API", "https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html"),
    ]
    story += [table(["Материал", "Описание"], sources,
                    [150, width - 150], font_size=8), Spacer(1, 10),
              para("Расчёты выполнены для варианта 11 с фильтром DevOps, DevSecOps, девопс, "
                   "SRE, Site Reliability и Platform Engineer. Формулировка отбора "
                   "приведена в разделе данных.")]

    destination.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(str(destination), pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm,
                            topMargin=17 * mm, bottomMargin=20 * mm,
                            title="Распределённый анализ вакансий DevOps",
                            author="Смирнов Александр", subject="RustFS, MapReduce и DuckDB")
    doc.build(story)
    reader = PdfReader(destination)
    page_text = [page.extract_text() or "" for page in reader.pages]
    full_text = "\n".join(page_text)
    required_sections = ["Данные и формирование сегмента", "Объектное хранилище",
                         "Обработка чанков и подсчёт", "Независимый подсчёт и проверка",
                         "Сравнение времени и памяти", "Навыки и медианная зарплата",
                         "Москва", "Санкт-Петербург", "Екатеринбург", "Выводы и ограничения", "Источники"]
    missing = [section for section in required_sections if section.casefold() not in full_text.casefold()]
    if missing or "Paragraph(" in full_text:
        raise RuntimeError(f"Report content check failed: {missing}")
    if "Sources" not in page_text[-1] and "Источники" not in page_text[-1]:
        raise RuntimeError("The sources must appear on the final page")
    quality = {
        "file": destination.relative_to(ROOT).as_posix(),
        "pages": len(reader.pages),
        "sections_present": required_sections,
        "sources_on_final_page": True,
        "checks_passed": True,
    }
    (ROOT / "evidence/report_quality.json").write_text(
        json.dumps(quality, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(quality, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--font-dir", type=Path, default=Path("C:/Windows/Fonts"))
    parser.add_argument("--out", type=Path, default=ROOT / "report/lab3_devops.pdf")
    args = parser.parse_args()
    make_report(args.font_dir, args.out)

