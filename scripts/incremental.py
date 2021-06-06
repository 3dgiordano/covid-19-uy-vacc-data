import json
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import gspread
import pandas as pd

monitor_url = 'https://monitor.uruguaysevacuna.gub.uy/plugin/cda/api/doQuery?'

schedule_url = "https://agenda.vacunacioncovid.gub.uy/data/schedule/schedule.json"

uy_init_cols = ["daily_vaccinated", "daily_coronavac", "daily_pfizer", "daily_astrazeneca",
                "people_coronavac", "people_pfizer", "people_astrazeneca",
                "fully_coronavac", "fully_pfizer", "fully_astrazeneca",
                "daily_agenda_ini", "daily_agenda", "daily_agenda_first", "daily_agenda_second",
                "total_ar", "total_ca", "total_cl", "total_co", "total_du", "total_fd", "total_fs",
                "total_la", "total_ma", "total_mo", "total_pa", "total_rn", "total_ro", "total_rv", "total_sa",
                "total_sj", "total_so", "total_ta", "total_tt",
                "people_ar", "people_ca", "people_cl", "people_co", "people_du", "people_fd", "people_fs",
                "people_la", "people_ma", "people_mo", "people_pa", "people_rn", "people_ro", "people_rv", "people_sa",
                "people_sj", "people_so", "people_ta", "people_tt",
                "fully_ar", "fully_ca", "fully_cl", "fully_co", "fully_du", "fully_fd", "fully_fs",
                "fully_la", "fully_ma", "fully_mo", "fully_pa", "fully_rn", "fully_ro", "fully_rv", "fully_sa",
                "fully_sj", "fully_so", "fully_ta", "fully_tt",
                ]

segment_init_cols = ["daily_teachers", "daily_elepem", "daily_chronic", "daily_undefined", "daily_dialysis",
                     "daily_health", "daily_deprived_liberty", "daily_essential", "daily_no_risk", "daily_pregnant"]

age_init_cols = [
    "daily_18_24", "daily_25_34", "daily_35_44", "daily_45_54", "daily_55_64", "daily_65_74", "daily_75_115",
    "daily_undefined", "daily_18_49", "daily_50_70", "daily_71_79", "daily_80_115"]

region_letter = {
    "A": "ar", "B": "ca", "C": "cl", "D": "co", "E": "du", "F": "fs", "G": "fd", "H": "la", "I": "ma", "J": "mo",
    "K": "pa", "L": "rn", "M": "rv", "N": "ro", "O": "sa", "P": "sj", "Q": "so", "R": "ta", "S": "tt", "X": "unk"
}

schedule_region_iso = {
    "1": "mo", "2": "ar", "3": "ca", "4": "cl", "5": "co", "6": "du", "7": "fs", "8": "fd",
    "9": "la", "10": "ma", "11": "pa", "12": "rn", "13": "rv", "14": "ro", "15": "sa",
    "16": "sj", "17": "so", "18": "ta", "19": "tt",
}

range_people = {
    "18_24": 367930,
    "25_34": 515451,
    "35_44": 478705,
    "45_54": 432164,
    "55_64": 385957,
    "65_74": 283920,
    "75_115": 240209
}


def find_row(date, data_dic):
    return [elem for elem in data_dic if elem["date"] == date]


def get_row_index(sheet_dic, row):
    return sheet_dic.index(row) + 2


def get_col_index(headers, label):
    return headers.index(label) + 1


def get_data(data, columns):
    json_origin = json.loads(urlopen(Request(monitor_url, data=data)).read().decode())
    return pd.DataFrame(json_origin["resultset"], columns=columns).fillna(0)


def get_data_schedule():
    json_origin = json.loads(urlopen(Request(schedule_url)).read().decode())
    return pd.DataFrame(json_origin)


def daily_vaccinated():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_evolucion&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['date', 'daily_vaccinated', 'daily_coronavac', 'daily_pfizer'])


def daily_vaccinated2():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_evolucion_tabla&outputIndexId=1&pageSize=0&pageStart=0&paramsearchBox="
    return get_data(data, ['date', 'first_dose', 'second_dose',
                           'sinovac_first_dose', 'sinovac_second_dose',
                           'pfizer_first_dose', 'pfizer_second_dose',
                           'astrazeneca_first_dose', 'astrazeneca_second_dose'
                           ])


def daily_doses():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_evolucion_dosis&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['date', 'first_dose', 'second_dose'])


def daily_vaccinated_by_age(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + \
           today_str + b"&paramp_rango_tipo=7&" \
                       b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2F" \
                       b"VacunasCovid.cda&dataAccessId=sql_vacunas_rango_edad&" \
                       b"outputIndexId=1&pageSize=0&pageStart=0&" \
                       b"sortBy=&paramsearchBox="
    result = get_data(data, ['age', 'value'])
    result.replace({'age': {
        u" años": u"",
        "No Definido": "undefined",
        "> 18 meses y <= 21 meses": "0",
        "4 meses": "0",
        "> 21 meses y <= 2": "0"
    }}, regex=True, inplace=True)

    daily_ages = {
        "18_24": 0, "25_34": 0, "35_44": 0, "45_54": 0, "55_64": 0, "65_74": 0, "75_115": 0, "undefined": 0,
        "18_49": 0, "50_70": 0, "71_79": 0, "80_115": 0
    }

    for age_index, age_row in result.iterrows():
        age = age_row["age"]
        age_key = "undefined"
        age_key2 = age_key
        age_int = 0 if age == "undefined" else int(age)
        if 18 <= age_int <= 24:
            age_key = "18_24"
        elif 25 <= age_int <= 34:
            age_key = "25_34"
        elif 35 <= age_int <= 44:
            age_key = "35_44"
        elif 45 <= age_int <= 54:
            age_key = "45_54"
        elif 55 <= age_int <= 64:
            age_key = "55_64"
        elif 65 <= age_int <= 74:
            age_key = "65_74"
        elif age_int >= 75:
            age_key = "75_115"

        daily_ages[age_key] += age_row["value"]

        if age_key != "undefined":
            if 18 <= age_int <= 49:
                age_key2 = "18_49"
            elif 50 <= age_int <= 70:
                age_key2 = "50_70"
            elif 71 <= age_int <= 79:
                age_key2 = "71_79"
            elif age_int >= 80:
                age_key2 = "80_115"

        daily_ages[age_key2] += age_row["value"]

    return daily_ages


def current_vaccinated_by_age():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_chart_vacunas_rango_edad&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&" \
           b"paramsearchBox="
    result = get_data(data, ['range', 'first', 'second'])
    result.replace({'range': {
        u"18 a 24": u"18_24",
        u"25 a 34": u"25_34",
        u"35 a 44": u"35_44",
        u"45 a 54": u"45_54",
        u"55 a 64": u"55_64",
        u"65 a 74": u"65_74",
        u"+ 75": u"75_115"
    }}, regex=False, inplace=True)
    return result.to_dict("records")


def region_vaccinated():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2F" \
           b"VacunasCovid.cda&dataAccessId=sql_vacunas_depto_vacunatorio_old&outputIndexId=1&pageSize=0&" \
           b"pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, [
        'code', 'p_first_dose', 'name', 'scale', 'first_dose', 'population', 'second_dose', 'p_second_dose'])


def region_vaccinated_residence():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2F" \
           b"VacunasCovid.cda&dataAccessId=sql_vacunas_depto_vacunatorio&outputIndexId=1&pageSize=0&" \
           b"pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, [
        'code', 'p_first_dose', 'name', 'scale', 'first_dose', 'population', 'second_dose', 'p_second_dose'])


def region_vaccination_centers():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_filtro_centro_vacuna&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['code', 'name'])


def date_agenda(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_indicadores_gral_agenda&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['future', 'today'])


def date_agenda_second_dose(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_indicadores_gral_agenda_dosis2&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['future', 'today'])


def today_status(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_indicadores_generales&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['total_vaccinations', 'today_vaccinations', 'first_dose', 'second_dose', 'update_time',
                           'country_doses'])


def segment_vaccination(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunas" \
           b"Covid.cda&dataAccessId=sql_vacunas_poblacion&outputIndexId=1&pageSize=0&pageStart=0&" \
           b"sortBy=&paramsearchBox="

    result = get_data(data, ['segment', 'vaccinations'])
    # Replace segment names
    result["segment"].replace({"Docentes": "teachers"}, inplace=True)
    result["segment"].replace({"ELEPEM": "elepem"}, inplace=True)
    result["segment"].replace({"Enfermedad crónica": "chronic"}, inplace=True)
    result["segment"].replace({"No Definido": "undefined"}, inplace=True)
    result["segment"].replace({"Pacientes en diálisis": "dialysis"}, inplace=True)
    result["segment"].replace({"Personal de salud": "health"}, inplace=True)
    result["segment"].replace({"Personas privadas de libertad": "deprived_liberty"}, inplace=True)
    result["segment"].replace({"Servicios esenciales": "essential"}, inplace=True)
    result["segment"].replace({"Sin factores de riesgo": "no_risk"}, inplace=True)
    result["segment"].replace({"Embarazadas": "pregnant"}, inplace=True)

    return result


def add_formatted_row(spreadsheet, sheet, date, init_cols):
    sheet_id = sheet.id
    last_row = len(list(filter(None, sheet.col_values(1))))
    last_col = sheet.col_count
    body = {
        'requests': [
            {
                'copyPaste': {
                    'source': {
                        'sheetId': sheet_id,
                        'startRowIndex': int(last_row - 1), 'endRowIndex': last_row,
                        'startColumnIndex': 0, 'endColumnIndex': last_col
                    },
                    'destination': {
                        'sheetId': sheet_id,
                        'startRowIndex': last_row, 'endRowIndex': int(last_row + 1),
                        'startColumnIndex': 0, 'endColumnIndex': last_col
                    },
                    'pasteType': 'PASTE_FORMULA'
                }
            }
        ]
    }
    spreadsheet.batch_update(body)  # Paste Formula
    body["requests"][0]["copyPaste"]["pasteType"] = "PASTE_FORMAT"
    spreadsheet.batch_update(body)

    sheet_headers = sheet.row_values(1)
    date_index = get_col_index(sheet_headers, "date")
    sheet.update_cell(int(last_row + 1), date_index, date)

    batch_update_cells = []
    for col_ini in init_cols:
        col_init_index = get_col_index(sheet_headers, col_ini)
        batch_update_cells.append(gspread.models.Cell(int(last_row + 1), col_init_index, value=0))
    if len(batch_update_cells) > 0:
        sheet.update_cells(batch_update_cells)


def transform_date(date_str):
    date_str = "-".join(date_str.split("-")[::-1])
    if not date_str.startswith("2021-"):
        date_str = "2021-" + date_str  # WA when format is without year
    return date_str


def update_minimal():
    updates = False

    daily_vac_origin = daily_doses().iloc[::-1]

    today = daily_vac_origin.head(1)["date"].values[0]  # transform_date(

    try:
        day_agenda_first = int(date_agenda(today)["today"].item() or 0)
        day_agenda_second = int(date_agenda_second_dose(today)["today"].item() or 0)
        day_agenda = day_agenda_first + day_agenda_second
    except HTTPError as e:
        print("Agenda error!")
        day_agenda_first = 0
        day_agenda_second = 0
        day_agenda = 0

    today_vac_status = today_status(today)

    daily_vac_origin_value = int(today_vac_status["today_vaccinations"])

    today_uodate_time = today + " " + today_vac_status["update_time"].values[0]

    # TODO: The api began to misinform the total, it is calculated with people first and second dose
    # today_total_vaccinations = int(today_vac_status["total_vaccinations"].item() or 0)
    today_total_people_vaccinations = int(today_vac_status["first_dose"].item() or 0)
    today_total_fully_vaccinations = int(today_vac_status["second_dose"].item() or 0)
    today_total_vaccinations = today_total_people_vaccinations + today_total_fully_vaccinations

    gc = gspread.service_account()
    sh = gc.open("CoronavirusUY - Vaccination monitor")

    sheet = sh.worksheet("Uruguay")
    sheet_dic = sheet.get_all_records()
    sheet_headers = sheet.row_values(1)

    sheet_schedule = sh.worksheet("Schedule")
    sheet_schedule_dic = sheet_schedule.get_all_records()
    sheet_schedule_headers = sheet_schedule.row_values(1)

    daily_people_vaccinated_col_index = get_col_index(sheet_headers, "people_vaccinated")
    daily_people_fully_vaccinated_col_index = get_col_index(sheet_headers, "people_fully_vaccinated")

    daily_vac_total_col_index = get_col_index(sheet_headers, "daily_vaccinated")

    daily_agenda_ini_col_index = get_col_index(sheet_headers, "daily_agenda_ini")
    daily_agenda_col_index = get_col_index(sheet_headers, "daily_agenda")

    daily_agenda_first_col_index = get_col_index(sheet_headers, "daily_agenda_first")
    daily_agenda_second_col_index = get_col_index(sheet_headers, "daily_agenda_second")

    last_row = sheet_dic[-1]
    last_date = last_row["date"]

    if last_date == today:
        if (today_total_vaccinations - last_row["total_vaccinations"]) < -1:
            print("* Execution Warning! Corrupt source data? Last valid:" + str(
                last_row["total_vaccinations"]) + " new:" + str(today_total_vaccinations))
            # return False

    batch_update_cells = []
    batch_update_schedule_cells = []

    for daily_vac_origin_index, daily_vac_origin_row in daily_vac_origin.iterrows():

        # Get date
        date_row = daily_vac_origin_row["date"]  # transform_date(

        sheet_row = find_row(date_row, sheet_dic)
        if len(sheet_row) == 0:  # If not exist, create the row

            if date_row == today:
                if (today_total_vaccinations - last_row["total_vaccinations"]) < -1:
                    print("* New date, Execution Warning! Corrupt source data? Last valid:" + str(
                        last_row["total_vaccinations"]) + " new:" + str(today_total_vaccinations))
                    # return False

            add_formatted_row(sh, sheet, date_row, uy_init_cols)
            time.sleep(2)  # Wait for refresh
            sheet_dic = sheet.get_all_records()  # Get updated changes
            sheet_row = find_row(date_row, sheet_dic)

            last_row = sheet_dic[-1]
            last_date = last_row["date"]

        sheet_daily_vac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_vaccinated"] or 0)

        daily_origin_first_dose = int(daily_vac_origin_row["first_dose"])
        daily_origin_second_dose = int(daily_vac_origin_row["second_dose"])

        sheet_row_index = -1 if len(sheet_row) == 0 else get_row_index(sheet_dic, sheet_row[0])

        sheet_agenda_ini = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda_ini"] or 0)
        sheet_agenda = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda"] or 0)
        sheet_agenda_first = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda_first"] or 0)
        sheet_agenda_second = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda_second"] or 0)

        sheet_people_vaccinated = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_vaccinated"] or 0)
        sheet_fully_vaccinations = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_fully_vaccinated"] or 0)

        if today == date_row:
            if sheet_agenda_ini == 0 and day_agenda > 0:
                # Set the ini agenda value
                print("Update Agenda ini:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda_ini) + " new:" + str(day_agenda))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_ini_col_index, value=day_agenda)
                )
            if sheet_agenda < day_agenda:
                print("Update Agenda:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda) + " new:" + str(day_agenda))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_col_index, value=day_agenda)
                )

            if sheet_agenda_first < day_agenda_first:
                print("Update Agenda First Dose:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda_first) + " new:" + str(day_agenda_first))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_first_col_index, value=day_agenda_first)
                )

            if sheet_agenda_second < day_agenda_second:
                print("Update Agenda Second Dose:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda_second) + " new:" + str(day_agenda_second))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_second_col_index, value=day_agenda_second)
                )

            # Update People vaccinated and Fully vaccinated
            if sheet_people_vaccinated != today_total_people_vaccinations:
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_people_vaccinated_col_index,
                                        value=today_total_people_vaccinations)
                )
            if sheet_fully_vaccinations != today_total_fully_vaccinations:
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_people_fully_vaccinated_col_index,
                                        value=today_total_fully_vaccinations)
                )

        if len(sheet_row) == 0:  # Extra control
            print("Create:" + date_row + " old: none new:" + str(daily_vac_origin_value))
        else:
            if sheet_daily_vac != daily_vac_origin_value:

                if today == date_row:
                    print("Update Daily:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                        sheet_daily_vac) + " new:" + str(daily_vac_origin_value))

                    if int(daily_vac_origin_value) < sheet_daily_vac:
                        print("* Warning! decrement!")

                    if int(daily_vac_origin_value) != sheet_daily_vac:
                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_total_col_index, value=daily_vac_origin_value)
                        )

            # Regions
            if today == date_row:

                daily_vac_region_origin = region_vaccinated()

                for daily_vac_region_origin_index, daily_vac_region_origin_row in daily_vac_region_origin.iterrows():
                    # Generate the label with the sheet format
                    region_id = daily_vac_region_origin_row["code"].split("-")[1].lower()
                    region_total_label = "total_" + region_id
                    region_people_label = "people_" + region_id
                    region_fully_label = "fully_" + region_id
                    sheet_total_vac_region = 0 if len(sheet_row) == 0 else int(sheet_row[0][region_total_label] or 0)

                    population = int(daily_vac_region_origin_row["population"].replace(".", ""))
                    daily_vac_region_origin_p_first_value = float(
                        daily_vac_region_origin_row["p_first_dose"].replace("%", "").replace(",", ".")) / 100

                    daily_vac_region_origin_p_second_value = float(
                        daily_vac_region_origin_row["p_second_dose"].replace("%", "").replace(",", ".")) / 100

                    daily_vac_region_origin_people_value = int(population * daily_vac_region_origin_p_first_value)
                    daily_vac_region_origin_fully_value = int(population * daily_vac_region_origin_p_second_value)
                    daily_vac_region_origin_total_value = daily_vac_region_origin_people_value
                    daily_vac_region_origin_total_value += daily_vac_region_origin_fully_value

                    if len(sheet_row) == 0:
                        print("Create Region:" + date_row + " " + region_total_label + " old: none new:" + str(
                            daily_vac_region_origin_total_value))
                    elif sheet_total_vac_region != daily_vac_region_origin_total_value:
                        daily_vac_region_total_col_index = get_col_index(sheet_headers, region_total_label)
                        daily_vac_region_people_col_index = get_col_index(sheet_headers, region_people_label)
                        daily_vac_region_fully_col_index = get_col_index(sheet_headers, region_fully_label)

                        print("Update Region:" + date_row + " " + region_total_label + " idx:" + str(
                            sheet_row_index) + " old:" + str(sheet_total_vac_region) + " new:" + str(
                            daily_vac_region_origin_total_value))

                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_region_total_col_index,
                                                value=daily_vac_region_origin_total_value)
                        )

                        # Update people and fully
                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_region_people_col_index,
                                                value=daily_vac_region_origin_people_value)
                        )
                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_region_fully_col_index,
                                                value=daily_vac_region_origin_fully_value)
                        )
                        if daily_vac_region_origin_total_value < sheet_total_vac_region:
                            print("* Warning! decrement! ")

            # Schedule
            if today == date_row:
                schedule = get_data_schedule()

                date_schedule = schedule.head(1)["timestamp"].values[0].split("T")[0]
                if date_row != date_schedule:
                    print(f"Schedule date not equeal than today {date_schedule} {date_row}")
                else:

                    sheet_schedule_row = find_row(date_row, sheet_schedule_dic)
                    if len(sheet_schedule_row) == 0:  # If not exist, create the row

                        add_formatted_row(sh, sheet_schedule, date_row, [])  # schedule_init_cols
                        time.sleep(2)  # Wait for refresh
                        sheet_schedule_dic = sheet_schedule.get_all_records()  # Get updated changes
                        sheet_schedule_row = find_row(date_row, sheet_schedule_dic)

                    sheet_schedule_row_index = -1 if len(sheet_schedule_row) == 0 else get_row_index(sheet_schedule_dic,
                                                                                                     sheet_schedule_row[
                                                                                                         0])
                    total_scheduled = 0
                    total_pending = 0
                    total_disabled = 0

                    for schedule_index, schedule_row in schedule.iterrows():
                        date_schedule = schedule_row["timestamp"].split("T")[0]

                        region_code = schedule_row["departaments"]["code"]

                        scheduled_label = "scheduled_" + schedule_region_iso[region_code]
                        scheduled_value = schedule_row["departaments"]["scheduled"]
                        total_scheduled += scheduled_value

                        daily_schedule_col_index = get_col_index(sheet_schedule_headers, scheduled_label)
                        batch_update_schedule_cells.append(
                            gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                                value=scheduled_value)
                        )

                        pending_label = "pending_" + schedule_region_iso[region_code]
                        pending_value = schedule_row["departaments"]["pending"]
                        total_pending += pending_value

                        daily_schedule_col_index = get_col_index(sheet_schedule_headers, pending_label)
                        batch_update_schedule_cells.append(
                            gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                                value=pending_value)
                        )

                        disabled_label = "disabled_" + schedule_region_iso[region_code]
                        disabled_value = schedule_row["departaments"]["disabled"]
                        total_disabled += disabled_value

                        daily_schedule_col_index = get_col_index(sheet_schedule_headers, disabled_label)
                        batch_update_schedule_cells.append(
                            gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                                value=disabled_value)
                        )

                    # update totals
                    daily_schedule_col_index = get_col_index(sheet_schedule_headers, "scheduled")
                    batch_update_schedule_cells.append(
                        gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                            value=total_scheduled)
                    )

                    daily_schedule_col_index = get_col_index(sheet_schedule_headers, "pending")
                    batch_update_schedule_cells.append(
                        gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                            value=total_pending)
                    )

                    daily_schedule_col_index = get_col_index(sheet_schedule_headers, "disabled")
                    batch_update_schedule_cells.append(
                        gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                            value=total_disabled)
                    )

    to_update_schedule = len(batch_update_schedule_cells)
    if to_update_schedule > 0:
        update_data = sheet_schedule.update_cells(batch_update_schedule_cells)
        # TODO: Implement a generic method to update batch of a sheet with retries

    to_update = len(batch_update_cells)
    if to_update > 0:
        updates = True
        update_data = sheet.update_cells(batch_update_cells)
        updated = update_data["updatedCells"]
        print("To update cells:" + str(to_update) + " Updated:" + str(updated))

        if to_update != updated:
            # Find difference and force to update
            address = []
            rows = []
            cols = []
            values = []
            for cell in batch_update_cells:
                rows.append(cell.row)
                cols.append(cell.col)
                address.append(cell.address)
                values.append(cell.value)

            ret_values = sheet.batch_get(address)
            force_cells = []
            for index, val in enumerate(ret_values):
                if int(val[0][0]) != values[index]:
                    print("Not updated:" + address[index] + " Val:" + str(values[index]) + " S:" + str(int(val[0][0])))
                    force_cells.append(
                        gspread.models.Cell(rows[index], cols[index], value=values[index])
                    )
            to_update = len(force_cells)
            if to_update > 0:
                update_data = sheet.update_cells(force_cells)
                updated = update_data["updatedCells"]
                print("Force to update cells:" + str(to_update) + " Updated:" + str(updated))

    # Refresh and check results
    time.sleep(2)  # Wait for refresh
    sheet_dic = sheet.get_all_records()  # Get updated changes
    last_row = sheet_dic[-1]
    last_date = last_row["date"]

    if last_date == today:
        print("Source Total:" + str(last_row["total_vaccinations"]) + " Final Total:" + str(today_total_vaccinations))

        # Update date time data
        sheet_data = sh.worksheet("Data")
        sheet_data.update_cell(6, 10, today_uodate_time)

    return updates


def update():
    # region_vacc_centers = region_vaccination_centers()

    # for region_vacc_center_index, region_vacc_center in region_vacc_centers.iterrows():
    #    name = region_vacc_center["name"]
    #    print(name)
    #    #print(name.split(" ")[0])

    #    # print(region_vacc_center)
    # return False

    updates = False

    daily_vac_origin = daily_vaccinated2()

    today = transform_date(daily_vac_origin.head(1)["date"].values[0])

    try:
        day_agenda_first = int(date_agenda(today)["today"].item() or 0)
        day_agenda_second = int(date_agenda_second_dose(today)["today"].item() or 0)
        day_agenda = day_agenda_first + day_agenda_second
    except HTTPError as e:
        print("Agenda error!")
        day_agenda_first = 0
        day_agenda_second = 0
        day_agenda = 0

    today_vac_status = today_status(today)

    today_uodate_time = today + " " + today_vac_status["update_time"].values[0]

    # TODO: The api began to misinform the total, it is calculated with people first and second dose
    # today_total_vaccinations = int(today_vac_status["total_vaccinations"].item() or 0)
    today_total_people_vaccinations = int(today_vac_status["first_dose"].item() or 0)
    today_total_fully_vaccinations = int(today_vac_status["second_dose"].item() or 0)
    today_total_vaccinations = today_total_people_vaccinations + today_total_fully_vaccinations

    gc = gspread.service_account()
    sh = gc.open("CoronavirusUY - Vaccination monitor")

    sheet = sh.worksheet("Uruguay")
    sheet_dic = sheet.get_all_records()
    sheet_headers = sheet.row_values(1)

    sheet_segment = sh.worksheet("Segment")
    sheet_segment_dic = sheet_segment.get_all_records()

    sheet_segment_headers = sheet_segment.row_values(1)
    last_segment_row = sheet_segment_dic[-1]

    sheet_age = sh.worksheet("Age")
    sheet_age_dic = sheet_age.get_all_records()

    sheet_age_headers = sheet_age.row_values(1)
    last_age_row = sheet_age_dic[-1]

    sheet_schedule = sh.worksheet("Schedule")
    sheet_schedule_dic = sheet_schedule.get_all_records()
    sheet_schedule_headers = sheet_schedule.row_values(1)

    daily_people_vaccinated_col_index = get_col_index(sheet_headers, "people_vaccinated")
    daily_people_fully_vaccinated_col_index = get_col_index(sheet_headers, "people_fully_vaccinated")

    daily_vac_total_col_index = get_col_index(sheet_headers, "daily_vaccinated")
    daily_coronavac_col_index = get_col_index(sheet_headers, "daily_coronavac")
    daily_pfizer_col_index = get_col_index(sheet_headers, "daily_pfizer")
    daily_astrazeneca_col_index = get_col_index(sheet_headers, "daily_astrazeneca")

    people_coronavac_col_index = get_col_index(sheet_headers, "people_coronavac")
    people_pfizer_col_index = get_col_index(sheet_headers, "people_pfizer")
    people_astrazeneca_col_index = get_col_index(sheet_headers, "people_astrazeneca")

    fully_coronavac_col_index = get_col_index(sheet_headers, "fully_coronavac")
    fully_pfizer_col_index = get_col_index(sheet_headers, "fully_pfizer")
    fully_astrazeneca_col_index = get_col_index(sheet_headers, "fully_astrazeneca")

    daily_agenda_ini_col_index = get_col_index(sheet_headers, "daily_agenda_ini")
    daily_agenda_col_index = get_col_index(sheet_headers, "daily_agenda")

    daily_agenda_first_col_index = get_col_index(sheet_headers, "daily_agenda_first")
    daily_agenda_second_col_index = get_col_index(sheet_headers, "daily_agenda_second")

    last_row = sheet_dic[-1]
    last_date = last_row["date"]

    if last_date == today:
        if (today_total_vaccinations - last_row["total_vaccinations"]) < -1:
            print("* Execution Warning! Corrupt source data? Last valid:" + str(
                last_row["total_vaccinations"]) + " new:" + str(today_total_vaccinations))
            # return False

    batch_update_cells = []
    batch_update_segment_cells = []
    batch_update_age_cells = []
    batch_update_schedule_cells = []

    for daily_vac_origin_index, daily_vac_origin_row in daily_vac_origin.iterrows():

        # Get date
        date_row = transform_date(daily_vac_origin_row["date"])

        sheet_row = find_row(date_row, sheet_dic)
        if len(sheet_row) == 0:  # If not exist, create the row

            if date_row == today:
                if (today_total_vaccinations - last_row["total_vaccinations"]) < -1:
                    print("* New date, Execution Warning! Corrupt source data? Last valid:" + str(
                        last_row["total_vaccinations"]) + " new:" + str(today_total_vaccinations))
                    # return False

            add_formatted_row(sh, sheet, date_row, uy_init_cols)
            time.sleep(2)  # Wait for refresh
            sheet_dic = sheet.get_all_records()  # Get updated changes
            sheet_row = find_row(date_row, sheet_dic)

            last_row = sheet_dic[-1]
            last_date = last_row["date"]

        sheet_daily_vac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_vaccinated"] or 0)

        sheet_daily_vac_coronavac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_coronavac"] or 0)
        sheet_people_vac_coronavac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_coronavac"] or 0)
        sheet_fully_vac_coronavac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["fully_coronavac"] or 0)

        sheet_daily_vac_pfizer = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_pfizer"] or 0)
        sheet_people_vac_pfizer = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_pfizer"] or 0)
        sheet_fully_vac_pfizer = 0 if len(sheet_row) == 0 else int(sheet_row[0]["fully_pfizer"] or 0)

        sheet_daily_vac_astrazeneca = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_astrazeneca"] or 0)
        sheet_people_vac_astrazeneca = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_astrazeneca"] or 0)
        sheet_fully_vac_astrazeneca = 0 if len(sheet_row) == 0 else int(sheet_row[0]["fully_astrazeneca"] or 0)

        people_vac_coronavac_origin_value = int(daily_vac_origin_row["sinovac_first_dose"].replace(".", ""))
        fully_vac_coronavac_origin_value = int(daily_vac_origin_row["sinovac_second_dose"].replace(".", ""))

        daily_vac_coronavac_origin_value = people_vac_coronavac_origin_value + fully_vac_coronavac_origin_value

        people_vac_pfizer_origin_value = int(daily_vac_origin_row["pfizer_first_dose"].replace(".", ""))
        fully_vac_pfizer_origin_value = int(daily_vac_origin_row["pfizer_second_dose"].replace(".", ""))

        daily_vac_pfizer_origin_value = people_vac_pfizer_origin_value + fully_vac_pfizer_origin_value

        people_vac_astrazeneca_origin_value = int(daily_vac_origin_row["astrazeneca_first_dose"].replace(".", ""))
        fully_vac_astrazeneca_origin_value = 0  # int(daily_vac_origin_row["astrazeneca_second_dose"].replace(".", ""))

        daily_vac_astrazeneca_origin_value = people_vac_astrazeneca_origin_value + fully_vac_astrazeneca_origin_value

        daily_vac_origin_value = daily_vac_coronavac_origin_value
        daily_vac_origin_value += daily_vac_pfizer_origin_value
        daily_vac_origin_value += daily_vac_astrazeneca_origin_value

        sheet_row_index = -1 if len(sheet_row) == 0 else get_row_index(sheet_dic, sheet_row[0])

        sheet_agenda_ini = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda_ini"] or 0)
        sheet_agenda = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda"] or 0)
        sheet_agenda_first = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda_first"] or 0)
        sheet_agenda_second = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda_second"] or 0)

        sheet_people_vaccinated = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_vaccinated"] or 0)
        sheet_fully_vaccinations = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_fully_vaccinated"] or 0)

        if today == date_row:
            if sheet_agenda_ini == 0 and day_agenda > 0:
                # Set the ini agenda value
                print("Update Agenda ini:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda_ini) + " new:" + str(day_agenda))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_ini_col_index, value=day_agenda)
                )
            if sheet_agenda < day_agenda:
                print("Update Agenda:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda) + " new:" + str(day_agenda))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_col_index, value=day_agenda)
                )

            if sheet_agenda_first < day_agenda_first:
                print("Update Agenda First Dose:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda_first) + " new:" + str(day_agenda_first))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_first_col_index, value=day_agenda_first)
                )

            if sheet_agenda_second < day_agenda_second:
                print("Update Agenda Second Dose:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda_second) + " new:" + str(day_agenda_second))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_second_col_index, value=day_agenda_second)
                )

            # Update People vaccinated and Fully vaccinated
            if sheet_people_vaccinated != today_total_people_vaccinations:
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_people_vaccinated_col_index,
                                        value=today_total_people_vaccinations)
                )
            if sheet_fully_vaccinations != today_total_fully_vaccinations:
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_people_fully_vaccinated_col_index,
                                        value=today_total_fully_vaccinations)
                )

        if len(sheet_row) == 0:  # Extra control
            print("Create:" + date_row + " old: none new:" + str(daily_vac_origin_value))
        else:
            if sheet_daily_vac != daily_vac_origin_value:

                print("Update:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_daily_vac) + " new:" + str(daily_vac_origin_value))

                if int(daily_vac_origin_value) < sheet_daily_vac:
                    print("* Warning! decrement!")

                if int(daily_vac_origin_value) != sheet_daily_vac:
                    batch_update_cells.append(
                        gspread.models.Cell(sheet_row_index, daily_vac_total_col_index, value=daily_vac_origin_value)
                    )

            if int(daily_vac_coronavac_origin_value) != sheet_daily_vac_coronavac:
                # Update daily vaccinated by type

                print("Update Daily Coronavac:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_daily_vac_coronavac) + " new:" + str(daily_vac_coronavac_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_coronavac_col_index,
                                        value=daily_vac_coronavac_origin_value)
                )

            if int(people_vac_coronavac_origin_value) != sheet_people_vac_coronavac:
                print("Update People Coronavac:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_people_vac_coronavac) + " new:" + str(people_vac_coronavac_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, people_coronavac_col_index,
                                        value=people_vac_coronavac_origin_value)
                )

            if int(fully_vac_coronavac_origin_value) != sheet_fully_vac_coronavac:
                print("Update Fully Coronavac:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_fully_vac_coronavac) + " new:" + str(fully_vac_coronavac_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, fully_coronavac_col_index,
                                        value=fully_vac_coronavac_origin_value)
                )

            if int(daily_vac_pfizer_origin_value) != sheet_daily_vac_pfizer:
                print("Update Pfizer:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_daily_vac_pfizer) + " new:" + str(daily_vac_pfizer_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_pfizer_col_index,
                                        value=daily_vac_pfizer_origin_value)
                )

            if int(people_vac_pfizer_origin_value) != sheet_people_vac_pfizer:
                print("Update People Pfizer:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_people_vac_pfizer) + " new:" + str(people_vac_pfizer_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, people_pfizer_col_index,
                                        value=people_vac_pfizer_origin_value)
                )

            if int(fully_vac_pfizer_origin_value) != sheet_fully_vac_pfizer:
                print("Update Fully Pfizer:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_fully_vac_pfizer) + " new:" + str(fully_vac_pfizer_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, fully_pfizer_col_index,
                                        value=fully_vac_pfizer_origin_value)
                )

            if int(daily_vac_astrazeneca_origin_value) != sheet_daily_vac_astrazeneca:
                print("Update AstraZeneca:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_daily_vac_astrazeneca) + " new:" + str(daily_vac_astrazeneca_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_astrazeneca_col_index,
                                        value=daily_vac_astrazeneca_origin_value)
                )

            if int(people_vac_astrazeneca_origin_value) != sheet_people_vac_astrazeneca:
                print("Update People AstraZeneca:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_people_vac_astrazeneca) + " new:" + str(people_vac_astrazeneca_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, people_astrazeneca_col_index,
                                        value=people_vac_astrazeneca_origin_value)
                )

            if int(fully_vac_astrazeneca_origin_value) != sheet_fully_vac_astrazeneca:
                print("Update Fully AstraZeneca:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_fully_vac_astrazeneca) + " new:" + str(fully_vac_astrazeneca_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, people_astrazeneca_col_index,
                                        value=people_vac_astrazeneca_origin_value)
                )

            # Segment
            if today == date_row:
                segment = segment_vaccination(date_row)
                sheet_segment_row = find_row(date_row, sheet_segment_dic)
                if len(sheet_segment_row) == 0:  # If not exist, create the row
                    add_formatted_row(sh, sheet_segment, date_row, segment_init_cols)
                    time.sleep(2)  # Wait for refresh
                    sheet_segment_dic = sheet_segment.get_all_records()  # Get updated changes
                    sheet_segment_row = find_row(date_row, sheet_segment_dic)

                sheet_segment_row_index = -1 if len(sheet_segment_row) == 0 else get_row_index(sheet_segment_dic,
                                                                                               sheet_segment_row[0])

                for daily_segment_origin_index, daily_segment_origin_row in segment.iterrows():
                    segment_label = "daily_" + daily_segment_origin_row["segment"]
                    segment_daily = int(daily_segment_origin_row["vaccinations"])

                    daily_segment_col_index = get_col_index(sheet_segment_headers, segment_label)

                    batch_update_segment_cells.append(
                        gspread.models.Cell(sheet_segment_row_index, daily_segment_col_index,
                                            value=segment_daily)
                    )

            # Age
            if today == date_row:
                daily_by_age = daily_vaccinated_by_age(date_row)
                sheet_age_row = find_row(date_row, sheet_age_dic)
                if len(sheet_age_row) == 0:  # If not exist, create the row
                    add_formatted_row(sh, sheet_age, date_row, age_init_cols)
                    time.sleep(2)  # Wait for refresh
                    sheet_age_dic = sheet_age.get_all_records()  # Get updated changes
                    sheet_age_row = find_row(date_row, sheet_age_dic)

                sheet_age_row_index = -1 if len(sheet_age_row) == 0 else get_row_index(sheet_age_dic,
                                                                                       sheet_age_row[0])

                for daily_age_key, daily_age_value in daily_by_age.items():
                    age_label = "daily_" + daily_age_key
                    age_daily = daily_age_value

                    daily_age_col_index = get_col_index(sheet_age_headers, age_label)
                    batch_update_age_cells.append(
                        gspread.models.Cell(sheet_age_row_index, daily_age_col_index,
                                            value=age_daily)
                    )
            # Age current progress
            if False:  # today == date_row:
                current_vacc_data_age = current_vaccinated_by_age()
                for range_elem in current_vacc_data_age:
                    range_age = range_elem["range"]
                    coverage_first = float(range_elem["first"]) / 100
                    coverage_first_label = "coverage_people_" + range_age
                    coverage_second = float(range_elem["second"]) / 100
                    coverage_second_label = "coverage_fully_" + range_age

                    label_col_index = get_col_index(sheet_age_headers, coverage_first_label)
                    batch_update_age_cells.append(
                        gspread.models.Cell(sheet_age_row_index, label_col_index,
                                            value=coverage_first)
                    )
                    label_col_index = get_col_index(sheet_age_headers, coverage_second_label)
                    batch_update_age_cells.append(
                        gspread.models.Cell(sheet_age_row_index, label_col_index,
                                            value=coverage_second)
                    )

            # Regions
            if today == date_row:

                daily_vac_region_origin = region_vaccinated()

                for daily_vac_region_origin_index, daily_vac_region_origin_row in daily_vac_region_origin.iterrows():
                    # Generate the label with the sheet format
                    region_id = daily_vac_region_origin_row["code"].split("-")[1].lower()
                    region_total_label = "total_" + region_id
                    region_people_label = "people_" + region_id
                    region_fully_label = "fully_" + region_id
                    sheet_total_vac_region = 0 if len(sheet_row) == 0 else int(sheet_row[0][region_total_label] or 0)

                    population = int(daily_vac_region_origin_row["population"].replace(".", ""))
                    daily_vac_region_origin_p_first_value = float(
                        daily_vac_region_origin_row["p_first_dose"].replace("%", "").replace(",", ".")) / 100

                    daily_vac_region_origin_p_second_value = float(
                        daily_vac_region_origin_row["p_second_dose"].replace("%", "").replace(",", ".")) / 100

                    daily_vac_region_origin_people_value = int(population * daily_vac_region_origin_p_first_value)
                    daily_vac_region_origin_fully_value = int(population * daily_vac_region_origin_p_second_value)
                    daily_vac_region_origin_total_value = daily_vac_region_origin_people_value
                    daily_vac_region_origin_total_value += daily_vac_region_origin_fully_value

                    if len(sheet_row) == 0:
                        print("Create Region:" + date_row + " " + region_total_label + " old: none new:" + str(
                            daily_vac_region_origin_total_value))
                    elif sheet_total_vac_region != daily_vac_region_origin_total_value:
                        daily_vac_region_total_col_index = get_col_index(sheet_headers, region_total_label)
                        daily_vac_region_people_col_index = get_col_index(sheet_headers, region_people_label)
                        daily_vac_region_fully_col_index = get_col_index(sheet_headers, region_fully_label)

                        print("Update Region:" + date_row + " " + region_total_label + " idx:" + str(
                            sheet_row_index) + " old:" + str(sheet_total_vac_region) + " new:" + str(
                            daily_vac_region_origin_total_value))

                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_region_total_col_index,
                                                value=daily_vac_region_origin_total_value)
                        )

                        # Update people and fully
                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_region_people_col_index,
                                                value=daily_vac_region_origin_people_value)
                        )
                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_region_fully_col_index,
                                                value=daily_vac_region_origin_fully_value)
                        )
                        if daily_vac_region_origin_total_value < sheet_total_vac_region:
                            print("* Warning! decrement! ")

            # Schedule
            if today == date_row:
                schedule = get_data_schedule()

                date_schedule = schedule.head(1)["timestamp"].values[0].split("T")[0]
                if date_row != date_schedule:
                    print(f"Schedule date not equeal than today {date_schedule} {date_row}")
                else:

                    sheet_schedule_row = find_row(date_row, sheet_schedule_dic)
                    if len(sheet_schedule_row) == 0:  # If not exist, create the row

                        add_formatted_row(sh, sheet_schedule, date_row, [])  # schedule_init_cols
                        time.sleep(2)  # Wait for refresh
                        sheet_schedule_dic = sheet_schedule.get_all_records()  # Get updated changes
                        sheet_schedule_row = find_row(date_row, sheet_schedule_dic)

                    sheet_schedule_row_index = -1 if len(sheet_schedule_row) == 0 else get_row_index(sheet_schedule_dic,
                                                                                                     sheet_schedule_row[
                                                                                                         0])
                    total_scheduled = 0
                    total_pending = 0
                    total_disabled = 0

                    for schedule_index, schedule_row in schedule.iterrows():
                        date_schedule = schedule_row["timestamp"].split("T")[0]

                        region_code = schedule_row["departaments"]["code"]

                        scheduled_label = "scheduled_" + schedule_region_iso[region_code]
                        scheduled_value = schedule_row["departaments"]["scheduled"]
                        total_scheduled += scheduled_value

                        daily_schedule_col_index = get_col_index(sheet_schedule_headers, scheduled_label)
                        batch_update_schedule_cells.append(
                            gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                                value=scheduled_value)
                        )

                        pending_label = "pending_" + schedule_region_iso[region_code]
                        pending_value = schedule_row["departaments"]["pending"]
                        total_pending += pending_value

                        daily_schedule_col_index = get_col_index(sheet_schedule_headers, pending_label)
                        batch_update_schedule_cells.append(
                            gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                                value=pending_value)
                        )

                        disabled_label = "disabled_" + schedule_region_iso[region_code]
                        disabled_value = schedule_row["departaments"]["disabled"]
                        total_disabled += disabled_value

                        daily_schedule_col_index = get_col_index(sheet_schedule_headers, disabled_label)
                        batch_update_schedule_cells.append(
                            gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                                value=disabled_value)
                        )

                    # update totals
                    daily_schedule_col_index = get_col_index(sheet_schedule_headers, "scheduled")
                    batch_update_schedule_cells.append(
                        gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                            value=total_scheduled)
                    )

                    daily_schedule_col_index = get_col_index(sheet_schedule_headers, "pending")
                    batch_update_schedule_cells.append(
                        gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                            value=total_pending)
                    )

                    daily_schedule_col_index = get_col_index(sheet_schedule_headers, "disabled")
                    batch_update_schedule_cells.append(
                        gspread.models.Cell(sheet_schedule_row_index, daily_schedule_col_index,
                                            value=total_disabled)
                    )

    to_update_segment = len(batch_update_segment_cells)
    if to_update_segment > 0:
        update_data = sheet_segment.update_cells(batch_update_segment_cells)
        # TODO: Implement a generic method to update batch of a sheet with retries

    to_update_age = len(batch_update_age_cells)
    if to_update_age > 0:
        update_data = sheet_age.update_cells(batch_update_age_cells)
        # TODO: Implement a generic method to update batch of a sheet with retries

    to_update_schedule = len(batch_update_schedule_cells)
    if to_update_schedule > 0:
        update_data = sheet_schedule.update_cells(batch_update_schedule_cells)
        # TODO: Implement a generic method to update batch of a sheet with retries

    to_update = len(batch_update_cells)
    if to_update > 0:
        updates = True
        update_data = sheet.update_cells(batch_update_cells)
        updated = update_data["updatedCells"]
        print("To update cells:" + str(to_update) + " Updated:" + str(updated))

        if to_update != updated:
            # Find difference and force to update
            address = []
            rows = []
            cols = []
            values = []
            for cell in batch_update_cells:
                rows.append(cell.row)
                cols.append(cell.col)
                address.append(cell.address)
                values.append(cell.value)

            ret_values = sheet.batch_get(address)
            force_cells = []
            for index, val in enumerate(ret_values):
                if int(val[0][0]) != values[index]:
                    print("Not updated:" + address[index] + " Val:" + str(values[index]) + " S:" + str(int(val[0][0])))
                    force_cells.append(
                        gspread.models.Cell(rows[index], cols[index], value=values[index])
                    )
            to_update = len(force_cells)
            if to_update > 0:
                update_data = sheet.update_cells(force_cells)
                updated = update_data["updatedCells"]
                print("Force to update cells:" + str(to_update) + " Updated:" + str(updated))

    # Refresh and check results
    time.sleep(2)  # Wait for refresh
    sheet_dic = sheet.get_all_records()  # Get updated changes
    last_row = sheet_dic[-1]
    last_date = last_row["date"]

    if last_date == today:
        print("Source Total:" + str(last_row["total_vaccinations"]) + " Final Total:" + str(today_total_vaccinations))

        # Update date time data
        sheet_data = sh.worksheet("Data")
        sheet_data.update_cell(6, 10, today_uodate_time)

    return updates


if __name__ == "__main__":
    limit_retry = 10
    num_retry = 1
    while num_retry <= limit_retry:
        print("Update:" + str(num_retry))
        if not update_minimal():
            print("Update finished")
            break
        print("Updated data, retrying to ensure no pending data...")
        num_retry += 1

    if num_retry > limit_retry:
        print("Retry limit reached")
