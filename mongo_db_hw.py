import pymongo
import csv
import re


def read_data(csv_file, db):
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row["Цена"] = int(row["Цена"])
            db.insert_one(row)
    print(f"Данные из {csv_file} записаны в коллекцию {db.name}")


def find_cheapest(db):
    # Сортировка
    print("\nСортировка исполнителей от меньшей к большей цене: ")
    for row in db.find().sort("Цена", 1):
        print(f"Исполнитель: {row['Исполнитель']}, (цена: {row['Цена']})")


def find_by_name(name, db):
    # Поиск подстроки
    print(f"\nСортировка исполнителей c подстрокой '{name}': ")
    regex = re.compile(fr"\w*\s*\w*({name})+\w*\s*\w*", re.IGNORECASE)
    for row in db.find({"Исполнитель": {"$regex": regex}}).sort("Цена", 1):
        print(f"Исполнитель: {row['Исполнитель']}, (цена: {row['Цена']})")


if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client["database"]
    db.collection = db["arrangement"]
    db.collection.delete_many({})
    read_data(csv_file="artists.csv", db=db.collection)
    find_cheapest(db=db.collection)
    find_by_name(name="tO", db=db.collection)
