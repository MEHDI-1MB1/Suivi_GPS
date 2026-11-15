# -*- coding: utf-8 -*-
"""Référentiels marocains : noms, sociétés, adresses, téléphones, CIN, plaques.
Tout passe par le générateur aléatoire `rng` (random.Random) -> résultats reproductibles avec --seed."""

FIRST_M = ["Mohamed", "Ahmed", "Youssef", "Hamza", "Omar", "Ayoub", "Mehdi", "Anas", "Soufiane", "Karim",
           "Rachid", "Abdelilah", "Said", "Hicham", "Khalid", "Abdelkrim", "Brahim", "Mustapha", "Driss",
           "Noureddine", "Jamal", "Abderrahim", "Yassine", "Zakaria", "Imad", "Redouane", "Tarik", "Nabil",
           "Fouad", "Hassan", "Lahcen", "Mohcine", "Abdelghani", "Adil", "Amine", "Badr", "Bilal", "Boubker",
           "Chakib", "Elhoussine", "Hafid", "Hamid", "Ilyas", "Ismail", "Jawad", "Khalil", "Larbi", "Marouane",
           "Mouad", "Nizar", "Othmane", "Rida", "Saad", "Salah", "Samir", "Taoufik", "Walid", "Younes",
           "Abdelaziz", "Abdellah", "Azzedine", "Abdessamad", "Aziz", "Hakim", "Mourad", "Miloud", "Lahbib",
           "Abdelmajid", "Mimoun", "Houssine", "Idriss", "Moulay Ali", "Taha", "Yahya"]
FIRST_F = ["Fatima", "Khadija", "Samira", "Nadia", "Houda", "Salma", "Imane", "Latifa", "Zineb", "Meriem",
           "Asmae", "Sanaa", "Hajar", "Loubna", "Naima", "Btissam", "Ghizlane", "Siham", "Kenza", "Ikram"]
# noms arabes et amazighes
LAST = ["El Amrani", "Alaoui", "Benjelloun", "Bennani", "Tazi", "Berrada", "Lahlou", "Chraibi", "El Idrissi",
        "Fassi", "El Fassi", "Bouzid", "Mansouri", "Haddad", "Saidi", "Benkirane", "Ait Taleb", "Oulhaj",
        "Boukhari", "Naciri", "Sebti", "Skalli", "Rifi", "Ziani", "Daoudi", "El Ouafi", "Belkadi", "Kabbaj",
        "Lamrani", "Mouline", "Slaoui", "Guerrouj", "Hajji", "Bouhssini", "Ait Ouakrim", "Azouggagh",
        "Id Boubker", "Oubella", "Ichou", "Aghbalou", "Ouchen", "Bouazza", "Zerouali", "Essaadi", "Benmoussa",
        "Lakhdar", "El Khattabi", "Tounsi", "Chakir", "El Mourabit", "Sabir", "Moujahid", "Ait Lhaj",
        "Ouchani", "Amghar", "Agouram", "Ouhammou", "Boutahar", "Mejdoubi", "Zahraoui", "El Garti", "Jebli",
        "Ghazi", "Rhazi", "El Yousfi", "Riffi", "Haddou", "Bakkali", "Cherkaoui", "El Harrak", "Filali",
        "Tahiri", "Qadiri", "Lahrichi", "Benabdellah", "Erraji", "El Bouhali", "Ouazzani", "Raji", "Sqalli",
        "Temsamani"]
BUS_PRE = ["Établissements", "Société", "Groupe", "Sté", "Maison"]
BUS_SUF = ["& Fils", "Distribution", "Négoce", "Logistique", "Industries", "Commerce", "Agro",
           "Import-Export", "SARL", "Matériaux", "Alimentaire"]
STREET_TYPES = ["Bd Mohammed VI", "Av. Hassan II", "Av. Mohammed V", "Bd Zerktouni", "Av. des FAR",
                "Bd Al Massira", "Rue Ibn Sina", "Rue Al Mouqawama", "Bd Moulay Youssef", "Av. Allal Ben Abdellah",
                "Route Nationale 1", "Rue Oued Fès", "Bd Abdelmoumen", "Av. Al Qods", "Rue Ibn Battouta"]
QUARTIERS = {
    "Casablanca": ["Maârif", "Aïn Sebaâ", "Sidi Maârouf", "Hay Hassani", "Bouskoura", "Aïn Chock", "Derb Sultan", "Roches Noires"],
    "Rabat": ["Agdal", "Hay Riad", "Souissi", "Yacoub El Mansour", "Océan", "Akkari"],
    "Marrakech": ["Guéliz", "Hivernage", "Sidi Ghanem", "Daoudiate", "Massira", "Azzouzia"],
    "Tanger": ["Malabata", "Boukhalef", "Iberia", "Mesnana", "Branes", "Zone Franche"],
    "Fès": ["Aïn Kadous", "Narjiss", "Route d'Imouzzer", "Dokkarat", "Sidi Brahim", "Zouagha"],
    "Agadir": ["Founty", "Talborjt", "Anza", "Hay Mohammadi", "Tassila", "Dcheira"],
    "El Jadida": ["Hay Salam", "Mazagan", "Sidi Bouzid", "Zone industrielle", "Hay Al Matar"],
    "Meknès": ["Hamria", "Toulal", "Marjane", "Zitoune", "Sidi Bouzekri", "Ismailia"],
    "Oujda": ["Hay Al Qods", "Lazaret", "Al Massira", "Sidi Yahya", "Zone industrielle"],
    "Kénitra": ["Ouled Oujih", "Saknia", "Mamora", "Bir Rami", "Zone industrielle"],
    "Safi": ["Jrifat", "Zone industrielle", "Sidi Ouassel", "Hay Hassani"],
    "Béni Mellal": ["Hay Al Qods", "Ouled Hamdane", "Zone industrielle", "Ain Asserdoun"],
    "Tétouan": ["Martil", "Wilaya", "Mhannech", "Zone industrielle", "Sania Ramel"],
    "Settat": ["Hay Al Qods", "Zone industrielle", "Hay Salam", "Hay Al Massira"],
    "Nador": ["Hay Al Matar", "Zone industrielle", "Ouled Mimoun", "Selouane"],
}
# indicatif régional de plaque (APPROXIMATIF : données synthétiques)
PLATE_REGION = {"Casablanca": [6, 26], "Rabat": [1, 4], "Tanger": [9], "Marrakech": [11], "Fès": [16],
                "Agadir": [5], "El Jadida": [10], "Meknès": [14], "Oujda": [13], "Kénitra": [3]}
PLATE_LETTERS = "ABDEHJTW"
CIN_LETTERS = ["A", "B", "BE", "BH", "BJ", "BK", "BL", "C", "CD", "D", "E", "F", "G", "H", "J", "K", "L", "M",
               "N", "P", "Q", "R", "S", "T", "U", "W", "X", "Z"]
# permis marocains : B, C, D, EB, EC, ED (CE n'existe pas au Maroc)
LICENSES = [["B"], ["B", "C"], ["B", "C", "EC"], ["B", "D"], ["B", "C", "D", "EC"]]


def person_name(rng, female_p=0.04):
    first = rng.choice(FIRST_F if rng.random() < female_p else FIRST_M)
    return "%s %s" % (first, rng.choice(LAST))


def company(rng):
    return "%s %s %s" % (rng.choice(BUS_PRE), rng.choice(LAST), rng.choice(BUS_SUF))


def address(rng, city):
    q = rng.choice(QUARTIERS.get(city, ["Centre-ville", "Zone industrielle"]))
    return "%d, %s, %s, %s" % (rng.randint(1, 320), rng.choice(STREET_TYPES), q, city)


def phone(rng):
    return "+212" + rng.choice(["6", "7"]) + "".join(str(rng.randint(0, 9)) for _ in range(8))


def cin(rng):
    return "%s%d" % (rng.choice(CIN_LETTERS), rng.randint(10000, 999999))


def plate(rng, city):
    return "%05d-%s-%d" % (rng.randint(1, 99999), rng.choice(PLATE_LETTERS), rng.choice(PLATE_REGION.get(city, [rng.randint(1, 89)])))
