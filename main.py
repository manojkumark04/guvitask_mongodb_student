import json
import pymongo as py

myclient = py.MongoClient("mongodb://127.0.0.1:27017/")
db = myclient["guvitask2"]
collection = db.guvitask2
f = open("C:/Users/User/Downloads/students.json")
data = json.load(f)

# if isinstance(data,list):
#     collection.insert_many(data)
# else:
#     collection.insert_one(data)

"""1.Maximum marks"""

for i in collection.aggregate([
    {"$project":{"_id":0, "name":1,"maximum_mark":{"$sum":"$scores.score"},}},
    {"$sort":{"maximum_mark":-1}},{"$limit":1}
]):
    # print("1. Student who scored maximum marks in all types is \n",i,"\n")
    pass

"""2. below average in exam"""

for av in collection.aggregate([{"$unwind":"$scores"},
                                {"$match":{"scores.type": "exam"}},
                                {"$group":{"_id":"$scores.type", "avge":{"$avg":"$scores.score"},"low":{"$lte":"avge"}}}
                                ]):
    pass

average_exam = (av["avge"])

print("2. students who scored below average marks and their marks in exam are below :\n")


for j in collection.aggregate([{"$unwind":"$scores"},
                                {"$match":{"scores.type": "exam"}},
                               {"$match":{"scores.score": {"$lte": average_exam}}},
                               {"$project":{"_id":0,"stduents_name_below_average_in_exam": "$name", "their_scores_in_exam": "$scores.score"}}]):
    pass
    # print(j)

print()
"""3. below pass mark and above pass mark"""

print("students who are all failed\n")

for fail in collection.aggregate([{"$match":{"scores.score": {"$lte": 40}}},
                               {"$project":{"_id":0,"student": "$name", "result": "Fail"}}]):
    pass
    # print(fail)

# print("students who are all passed\n")

#  not solved

"""3. Above pass mark"""

print("students who are all passed\n")


for passed in collection.aggregate([{"$unwind":"$scores"},
                               {"$match":{"scores.type": "exam","scores.score": {"$gte": 40},
                                                  "scores.type": "quiz","scores.score": {"$gte": 40},
                                                  "scores.type": "homework","scores.score": {"$gte": 40}}},
#                                {"$match":{"scores.type": "quiz","scores.score": {"$gt": 40}}},
#                                {"$match":{"scores.type": "homework","scores.score": {"$gt": 40}}},
                               {"$project":{"_id":1, "student_name": "$name","result": "passed"}}]):
    print(passed)


"""4. total and average"""


for total in collection.aggregate([{"$unwind":"$scores"},
                             {"$group": {"_id": "$scores.type", "total": {"$sum":"$scores.score"}}},
                            # {"$out":"Total_marks"}
                             ]):
    pass

for average in collection.aggregate([{"$unwind":"$scores"},
                             {"$group": {"_id": "$scores.type", "Average": {"$avg":"$scores.score"}}},
                            # {"$out":"Average_marks"}
                             ]):
    pass


"""5. """
for avereges in collection.aggregate([{"$unwind":"$scores"},
                               {"$match":{"scores.type": "exam"}},
                               {"$match":{"scores.score": {"$lte": 40}}},
                               {"$project":{"_id":0,"stduents_name_below_average_in_exam": "$name"}}]):
    # print(avereges)
    pass



"""6. all below 40% fail"""


# for exam_fails in collection.aggregate([{"$unwind":"$scores"},
#                                {"$match":{"scores.type": "exam"}},
#                                {"$match":{"scores.score": {"$lte": 40}}},
#                                {"$project":{"_id":0,"stduents_name_below_passmark_in_exam": "$name","their_scores_in_exam": "$scores.score"}},
#                                {"$out": "failure"},
#                                 ]):
#     # print(exam_fails)
#     pass
#
# for quiz_fails in collection.aggregate([{"$unwind":"$scores"},
#                                {"$match":{"scores.type": "quiz"}},
#                                {"$match":{"scores.score": {"$lte": 40}}},
#                                {"$project":{"_id":0,"stduents_name_below_passmark_in_quiz": "$name","their_scores_in_quiz": "$scores.score"}},
#                                # {"$out": "failure"}
#                                 ]):
#     # print(quiz_fails)
#     pass
#
#     quizfails = db.failure.insert_one(quiz_fails)

#
# for homework_fails in collection.aggregate([{"$unwind":"$scores"},
#                                {"$match":{"scores.type": "homework"}},
#                                {"$match":{"scores.score": {"$lte": 40}}},
#                                {"$project":{"_id":0,"stduents_name_below_passmark_in_homework": "$name","their_scores_in_homework": "$scores.score"}},
#                                 ]):
#     homeworkfails = db.failure.insert_one(homework_fails)
for fjak in db.failure.find():
    print(fjak)

#
#
"""7. all above 40% fail"""