import pandas as pd

# Enable share with link to allow export to public
base_seet = "https://docs.google.com/spreadsheets/d/" \
            "1J-cruSsRqJxaY6rPjwK7tWdIAldWfR82GH9Pp0u-WD0/gviz/tq?tqx=out:csv&sheet="


def uruguay():
    df = pd.read_csv(
        f"{base_seet}Uruguay"
    )

    df.to_csv("../data/Uruguay.csv", index=False)


def regions():
    df = pd.read_csv(
        f"{base_seet}Regions"
    )

    df.to_csv("../data/Regions.csv", index=False)


def subnational():
    # Subnational COVID-19 vaccination data format
    # https://github.com/sociepy/covid19-vaccination-subnational

    df = pd.read_csv(
        f"{base_seet}Uruguay"
    )
    region_id = {
        "UY-AR": "Artigas",
        "UY-CA": "Canelones",
        "UY-CL": "Cerro Largo",
        "UY-CO": "Colonia",
        "UY-DU": "Durazno",
        "UY-FS": "Flores",
        "UY-FD": "Florida",
        "UY-LA": "Lavalleja",
        "UY-MA": "Maldonado",
        "UY-MO": "Montevideo",
        "UY-PA": "Paysandu",
        "UY-RN": "Rio Negro",
        "UY-RV": "Rivera",
        "UY-RO": "Rocha",
        "UY-SA": "Salto",
        "UY-SJ": "San Jose",
        "UY-SO": "Soriano",
        "UY-TA": "Tacuarembo",
        "UY-TT": "Treinta y Tres",
    }
    location_iso = "UY"
    regions_data = []
    for index, element in df.iterrows():
        date = element["date"]
        location = element["location"]
        for region_iso, region in region_id.items():
            r_subkey = region_iso.split("-")[1].lower()
            total = element["total_" + r_subkey]
            people = element["people_" + r_subkey]
            fully = element["fully_" + r_subkey]
            regions_data.append(
                {
                    "location": location,
                    "region": region,
                    "date": date,
                    "location_iso": location_iso,
                    "region_iso": region_iso,
                    "total_vaccinations": total,
                    "people_vaccinated": people,
                    "people_fully_vaccinated": fully

                }
            )

    df_subnational = pd.DataFrame(regions_data)
    df_subnational.to_csv("../data/Subnational.csv", index=False)


def segments():
    df = pd.read_csv(
        f"{base_seet}Segment"
    )

    df.to_csv("../data/Segments.csv", index=False)


def age():
    df = pd.read_csv(
        f"{base_seet}Age"
    )

    df.to_csv("../data/Age.csv", index=False)


def schedule():
    df = pd.read_csv(
        f"{base_seet}Schedule"
    )

    df.to_csv("../data/Schedule.csv", index=False)


def deaths():
    df = pd.read_csv(
        f"{base_seet}Deaths"
    )

    df.to_csv("../data/Deaths.csv", index=False)



def main():
    uruguay()
    regions()
    segments()
    subnational()
    age()
    schedule()
    deaths()


if __name__ == "__main__":
    main()
