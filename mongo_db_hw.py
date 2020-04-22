import pymongo
import csv
import re


def read_data(csv_file, db):
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        db.insert_many(reader)
    print(f"Данные из {csv_file} записаны в коллекцию {db.name}")


def string_to_int(db):
    # Приведение строки в число для функции sort
    string_treated = 0
    if string_treated == 0:
        for row in db.find():
            old_price = {"Цена": row["Цена"]}
            new_price = {"$set": {"Цена": int(row["Цена"])}}
            db.update_one(old_price, new_price)
        string_treated += 1


def find_cheapest(db):
    string_to_int(db=db)
    # Сортировка
    print("\nСортировка исполнителей от меньшей к большей цене: ")
    for row in db.find().sort("Цена", 1):
        print(f"Исполнитель: {row['Исполнитель']}, (цена: {row['Цена']})")


def find_by_name(name, db):
    string_to_int(db=db)
    # Поиск подстроки
    print(f"\nСортировка исполнителей c подстрокой '{name}': ")
    for row in db.find().sort("Цена", 1):
        pattern = re.compile(f"\w*\s*\w*({name.lower()})+\w*\s*\w*")
        result = pattern.findall(row['Исполнитель'].lower())
        if result:
            print(f"Исполнитель: {row['Исполнитель']}, (цена: {row['Цена']})")
        # else:
        #     print("Результаты поиска не дали результатов")


if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client["database"]
    db.collection = db["arrangement"]
    db.collection.delete_many({})
    read_data(csv_file="artists.csv", db=db.collection)
    find_cheapest(db=db.collection)
    find_by_name(name="tO", db=db.collection)
