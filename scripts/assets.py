import concurrent.futures
import os
import time
import urllib.request as req

base_url = "https://docs.google.com/spreadsheets/d/e/"
sheet = "2PACX-1vRSB3_JCKkvYQkgEwYW0PkzMJDovwvMwX28B5ainGuDirimi6n4n1nryc0Pbb0fHCfsZVYAnqobgP8D"


def save_img(oid):
    img_url = f"{base_url}{sheet}/pubchart?oid={oid}&format=image"
    image_file = os.path.abspath(f"../web/charts/{oid}.png")
    if os.path.exists(image_file):
        os.remove(image_file)
    req.urlretrieve(img_url, image_file)


images = [
    "574263984", "1320291746",
    "1939554456", "141578891",
    "744871918", "2074125212",
    "1201195179", "1744392307",
    "857919432", "425905901",
    "851362461", "395420450",
    "1329486679", "1492441660",
    "827148403", "22096209",
    "1848022003", "873256307",
    "2077796175", "1058712731",
    "1978363820", "1357338484",
    "1038688506", "591742088",
    "603335823", "1958520312",
    "1683681566", "373318070",
    "2063902375", "1924052371",
    "1821951025", "1074834619",
    "1322547223", "682972572",
    "731574492", "119707745",
    "842189614", "1158305404",
    "1047100711", "1781225090",
    "132326038", "259061157",
    "122662822", "121562673",
    "987529461", "1299383115",
    "454080210", "1279668502",
    "105471492", "1092961723",
    "2095693594", "134138183",
    "648030237", "861619954",
    "1720013265", "736411819",
    "1189791692", "824007235",
    "4849953", "1785941673",
    "784232676", "661817159",
    "1133283679", "1140302154",
    "1880002572", "1515424457",
    "1740954658", "655562320",
    "446851537", "1541211770",
    "75622886", "1656096582",
    "1972410624", "770096093",
    "1466475246", "1082865934",
    "1898654916", "1598348414",
    "1576723715", "519951936",
    "205213491", "404093750"

]

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(save_img, img): img for img in images}

    for future in concurrent.futures.as_completed(futures):
        image_arg = futures[future]
        try:
            result = future.result()
        except Exception as exc:
            print("Saving image {} generated an exception: {}".format(image_arg, exc))
        else:
            print("Image {} saved successfully.".format(image_arg))

# Paused to give git time to recognize changes
time.sleep(2)
