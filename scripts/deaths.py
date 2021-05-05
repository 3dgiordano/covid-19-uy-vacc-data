import datetime
import time

import gspread
import pandas as pd

deaths_url = "https://github.com/GUIAD-COVID/datos-y-visualizaciones-GUIAD/raw/master/datos/" \
             "estadisticasUY_fallecimientos.csv"


def find_row(date, data_dic):
    return [elem for elem in data_dic if elem["date"] == date]


def get_row_index(sheet_dic, row):
    return sheet_dic.index(row) + 2


def get_col_index(headers, label):
    return headers.index(label) + 1


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


def daily_deaths_by_age():
    result = pd.read_csv(deaths_url)

    daily_dates = {}

    dt_prev = datetime.datetime(2021, 2, 26)
    dt_from = datetime.datetime(2021, 2, 27)  # Vaccination begins
    dt_to = datetime.datetime.today() - datetime.timedelta(1)

    daily_dates["PREV"] = {
        "18_24": 0, "25_34": 0, "35_44": 0, "45_54": 0, "55_64": 0, "65_74": 0, "75_115": 0, "undefined": 0,
        "18_49": 0, "50_70": 0, "71_79": 0, "80_115": 0
    }

    dates_list = pd.date_range(start=dt_from, end=dt_to).to_pydatetime().tolist()
    for date_dt in dates_list:
        date_dt_str = date_dt.strftime("%Y-%m-%d")
        daily_dates[date_dt_str] = {
            "18_24": 0, "25_34": 0, "35_44": 0, "45_54": 0, "55_64": 0, "65_74": 0, "75_115": 0, "undefined": 0,
            "18_49": 0, "50_70": 0, "71_79": 0, "80_115": 0
        }

    for age_index, age_row in result.iterrows():

        ds = age_row["fecha"].split("/")
        date = f"{ds[2]}-{ds[1]}-{ds[0]}"

        dt = datetime.datetime(int(ds[2]), int(ds[1]), int(ds[0]))

        if dt <= dt_prev:
            date = "PREV"

        if date not in daily_dates:
            daily_dates[date] = {
                "18_24": 0, "25_34": 0, "35_44": 0, "45_54": 0, "55_64": 0, "65_74": 0, "75_115": 0, "undefined": 0,
                "18_49": 0, "50_70": 0, "71_79": 0, "80_115": 0
            }

        age = age_row["edad"]
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

        daily_dates[date][age_key] += 1

        if age_key != "undefined":
            if 18 <= age_int <= 49:
                age_key2 = "18_49"
            elif 50 <= age_int <= 70:
                age_key2 = "50_70"
            elif 71 <= age_int <= 79:
                age_key2 = "71_79"
            elif age_int >= 80:
                age_key2 = "80_115"

        daily_dates[date][age_key2] += 1

    return daily_dates


gc = gspread.service_account()
sh = gc.open("CoronavirusUY - Vaccination monitor")

sheet_deaths = sh.worksheet("DeathAges")
sheet_deaths_dic = sheet_deaths.get_all_records()
sheet_deaths_headers = sheet_deaths.row_values(1)

batch_update_deaths_cells = []

# Deaths
deaths = daily_deaths_by_age()

for deaths_date in deaths:

    if deaths_date != "PREV":
        deaths_data = deaths[deaths_date]

        sheet_deaths_row = find_row(deaths_date, sheet_deaths_dic)
        if len(sheet_deaths_row) == 0:  # If not exist, create the row
            add_formatted_row(sh, sheet_deaths, deaths_date, [])
            time.sleep(2)  # Wait for refresh
            sheet_deaths_dic = sheet_deaths.get_all_records()  # Get updated changes
            sheet_deaths_row = find_row(deaths_date, sheet_deaths_dic)

        sheet_deaths_row_index = -1 if len(sheet_deaths_row) == 0 else get_row_index(sheet_deaths_dic,
                                                                                     sheet_deaths_row[0])

        for daily_death_key, daily_death_value in deaths_data.items():
            death_label = "daily_" + daily_death_key
            death_daily = daily_death_value
            daily_death_col_index = get_col_index(sheet_deaths_headers, death_label)
            batch_update_deaths_cells.append(
                gspread.models.Cell(sheet_deaths_row_index, daily_death_col_index,
                                    value=death_daily)
            )
    else:
        print(deaths[deaths_date])

to_update_deaths = len(batch_update_deaths_cells)
if to_update_deaths > 0:
    update_data = sheet_deaths.update_cells(batch_update_deaths_cells)
    # TODO: Implement a generic method to update batch of a sheet with retries
