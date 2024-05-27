import random

country_mappings = {
        "US": "en-US,en;q=0.9",
        "GB": "en-GB,en;q=0.9",
        "DE": "de-DE,de;q=0.9",
        "FR": "fr-FR,fr;q=0.9",
        "ES": "es-ES,es;q=0.9",
        "IT": "it-IT,it;q=0.9",
        "RU": "ru-RU,ru;q=0.9",
        "CN": "zh-CN,zh;q=0.9",
        "JP": "ja-JP,ja;q=0.9",
        "KR": "ko-KR,ko;q=0.9",
        "BR": "pt-BR,pt;q=0.9",
        "PT": "pt-PT,pt;q=0.9",
        "NL": "nl-NL,nl;q=0.9",
        "BE": "nl-BE,fr-BE;q=0.9",
        "IN": "hi-IN,hi;q=0.9,en-IN,en;q=0.8",
        "TR": "tr-TR,tr;q=0.9",
        "SA": "ar-SA,ar;q=0.9",
        "AE": "ar-AE,ar;q=0.9",
        "EG": "ar-EG,ar;q=0.9",
        "SE": "sv-SE,sv;q=0.9",
        "NO": "nb-NO,nn-NO,no;q=0.9",
        "DK": "da-DK,da;q=0.9",
        "FI": "fi-FI,fi;q=0.9",
        "PL": "pl-PL,pl;q=0.9",
        "CZ": "cs-CZ,cs;q=0.9",
        "SK": "sk-SK,sk;q=0.9",
        "HU": "hu-HU,hu;q=0.9",
        "RO": "ro-RO,ro;q=0.9",
        "GR": "el-GR,el;q=0.9",
        "IL": "he-IL,he;q=0.9",
        "TH": "th-TH,th;q=0.9",
        "VN": "vi-VN,vi;q=0.9",
        "MY": "ms-MY,ms;q=0.9",
        "ID": "id-ID,id;q=0.9",
        "PH": "tl-PH,tl;q=0.9,en-PH,en;q=0.8",
        "AU": "en-AU,en;q=0.9",
        "CA": "en-CA,fr-CA;q=0.9",
        "ZA": "af-ZA,af;q=0.9,en-ZA,en;q=0.8",
        "NG": "en-NG,en;q=0.9",
        "GH": "en-GH,en;q=0.9",
        "KE": "en-KE,sw-KE;q=0.9",
    }

def get_random_chrome_version_details():
    chrome_versions = [
        {"major_version": "120", "full_version": "120.0.6099.110", "version": "120"},
        {"major_version": "120", "full_version": "120.0.6099.71", "version": "120"},
        {"major_version": "120", "full_version": "120.0.6099.63", "version": "120"},
        {"major_version": "120", "full_version": "120.0.6099.62", "version": "120"},
        {"major_version": "120", "full_version": "120.0.6099.130", "version": "120"},
        {"major_version": "120", "full_version": "120.0.6099.200", "version": "120"},
        {"major_version": "120", "full_version": "120.0.6099.217", "version": "120"},
        {"major_version": "120", "full_version": "120.0.6099.225", "version": "120"},
        {"major_version": "119", "full_version": "119.0.6045.200", "version": "119"},
        {"major_version": "119", "full_version": "119.0.6045.160", "version": "119"},
        {"major_version": "119", "full_version": "119.0.6045.124", "version": "119"},
        {"major_version": "119", "full_version": "119.0.6045.106", "version": "119"},
    ]

    return random.choice(chrome_versions)

def get_random_windows_nt_version():
    windows_nt_versions = ['10.0', '6.1', '6.2', '6.3']
    return random.choice(windows_nt_versions)

def get_random_architecture_details():
    arch = random.choice(["x86", "x64"])
    bitness = "64" if arch == "x64" else "32"
    return arch, bitness

def get_random():
    chrome_version_details = get_random_chrome_version_details()
    windows_nt_version = get_random_windows_nt_version()
    arch, bitness = get_random_architecture_details()

    return chrome_version_details, windows_nt_version, arch, bitness
