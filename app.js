const { MongoClient } = require('mongodb');

// Nazwa bazy danych
const dbName = 'IMDB';
// URL do bazy danych MongoDB
const url = 'mongodb://127.0.0.1:27017';
const client = new MongoClient(url);
const db = client.db(dbName);

const titleCollection = db.collection('Title');
const ratingCollection = db.collection('Rating');
const nameCollection = db.collection('Name');


client.connect();
console.log('Connected with MongoDB');

async function Zadanie1() {

    const titleCount = await titleCollection.countDocuments();
    console.log(`Title count: `, titleCount);

    const ratingCount = await ratingCollection.countDocuments();
    console.log(`Rating count: `, ratingCount);

    const nameCount = await nameCollection.countDocuments();
    console.log(`Name count: `, nameCount);
    client.close();
}

async function Zadanie2() {
    try {

        const firstPart = await titleCollection.
            find({
                startYear: 2020, genres: { $regex: "Romance" }, runtimeMinutes: { $gt: 90, $lte: 120 }
            }).
            limit(4).sort({ primaryTitle: 1 }).
            project({ primaryTitle: 1, startYear: 1, runtimeMinutes: 1, genres: 1, _id: 0 }).toArray();
        console.log(firstPart)


        const secondPart = await titleCollection.
            countDocuments({
                startYear: 2020, genres: { $regex: "Romance" }, runtimeMinutes: { $gt: 90, $lte: 120 }
            });
        console.log("Number of movies: " + secondPart)



    } finally {
        await client.close();
    }
}
async function Zadanie3() {
    try {

        const typesIn2000 = titleCollection.aggregate([
            { $match: { startYear: 2000 } },
            { $group: { _id: "$titleType", total: { $sum: 1 } } }
        ]);

        const resultsArray = await typesIn2000.toArray();
        for (const titleType of resultsArray) {
            console.log(titleType);
        }

    } finally {
        await client.close();
    }
}
// Stworzenie indexów, żeby przyspieszyć zad4
// Indeks na startYear i genres w kolekcji Title
//  titleCollection.createIndex({ startYear: 1, genres: 1 });

// // Indeks na tconst w kolekcji Title
//  titleCollection.createIndex({ tconst: 1 });

// // Indeks na tconst w kolekcji Rating
//  ratingCollection.createIndex({ tconst: 1 });

async function Zadanie4() {
    try {

        const FIVEdocumentary2010_12 = await titleCollection.aggregate([
            {
                $match: {
                    startYear: { $in: [2010, 2011, 2012] },
                    genres: { $regex: "Documentary", $options: "i" }
                }
            },
            {
                $lookup: {
                    from: "Rating",
                    localField: "tconst",
                    foreignField: "tconst",
                    as: "joinRating"
                }
            },
            {
                $unwind: "$joinRating"
            },
            {
                $project: {
                    primaryTitle: 1,
                    startYear: 1,
                    avgRating: "$joinRating.averageRating",
                    _id: 0
                }
            },
            {
                $sort: { avgRating: -1 }
            },
            {
                $limit: 5
            }
        ]).toArray();
        
        console.log(FIVEdocumentary2010_12);
        
        // const resultsArray = await documentary2010_12.toArray();
        // for (const titleType of resultsArray) {
        //     console.log(titleType);
        // }
    } finally {
        await client.close();
    }
}
 Zadanie4()

// nameCollection.createIndex({primaryName:"text"})
// const nameIndexes = await nameCollection.indexes();
// console.log(nameIndexes)
async function Zadanie5() {
    try {
        const FondaOrCopollaCount = await nameCollection.countDocuments({
            $text: { $search: "Fonda Coppola", $caseSensitive: true }
        })
        console.log("Fonda and Coppola count: " + FondaOrCopollaCount);

        const FondaOrCopollaFirst5 = await nameCollection.find({
            $text: { $search: "Fonda Coppola", $caseSensitive: true }
        }).project({ primaryName: 1, primaryProfession: 1, _id: 0 }).limit(5).toArray()

        console.log("Five first documents: ")
        console.log(FondaOrCopollaFirst5);

    } finally {
        await client.close();
    }
}



async function Zadanie6() {
    try {
        //    await nameCollection.createIndex({ birthYear: -1 });

        const nameIndexes = await nameCollection.indexes();
        console.log("Name indexes: ");
        console.log(nameIndexes);

        console.log("Name indexes count: " + nameIndexes.length)


    } finally {
        await client.close();
    }
}


async function Zadanie7() {
    try {

        // const topRatedMovies = await ratingCollection.find({ averageRating: 10.0 }).toArray();
        // const tconstList = topRatedMovies.map(movie => movie.tconst);

        // const updateResult = await titleCollection.updateMany(
        //   { tconst: { $in: tconstList } },
        //   { $set: { max: 1 } }
        // );
        const topRatedMoviesInTitle = await titleCollection.find({ max: 1 }).toArray();
        console.log(topRatedMoviesInTitle)
    } finally {
        await client.close();
    }
}

async function Zadanie8() {
    try {
        const TheSea1895 = await titleCollection.aggregate([
            { $match: { startYear: 1895, primaryTitle: { $regex: "The Sea" } } },
            {
                $lookup: {
                    from: "Rating",
                    localField: "tconst",
                    foreignField: "tconst",
                    as: "joinRating"
                }
            }]
        ).project({ primaryTitle: 1, startYear: 1, _id: 0, averageRating: '$joinRating.averageRating' }).toArray();

        console.log(TheSea1895)
    } finally {
        await client.close();
    }
}

async function Zadanie9() {
    try {
        const BladeRunner1982 = await titleCollection.aggregate([
            { $match: { startYear: 1982, primaryTitle: { $regex: "^Blade Runner$" } } },
            {
                $lookup: {
                    from: "Rating",
                    localField: "tconst",
                    foreignField: "tconst",
                    as: "joinRating"
                }
            }]).toArray()
        // console.log(BladeRunner1982)

        // const newField = [];
        // let numVotes = BladeRunner1982[0].joinRating[0].numVotes;
        // let avgRating = BladeRunner1982[0].joinRating[0].averageRating;
        // newField.push({averageRating:avgRating,numVotes:numVotes})
        // console.log(newField)

        // await titleCollection.updateOne({primaryTitle:"Blade Runner",startYear:1982},{$set:{rating:newField}})

        const BladeRunner1982AfterUpdate = await titleCollection.findOne({ primaryTitle: "Blade Runner", startYear: 1982 });
        console.log(BladeRunner1982AfterUpdate);

    } finally {
        await client.close();
    }
}

async function Zadanie10() {
    try {

        await titleCollection.updateOne(
            { primaryTitle: "Blade Runner", startYear: 1982 },
            { $push: { rating: { averageRating: 10, numVotes: 55555 } } }
        );

        const BladeRunner1982AfterUpdate = await titleCollection.findOne({ primaryTitle: "Blade Runner", startYear: 1982 });
        console.log(BladeRunner1982AfterUpdate);

    } finally {
        await client.close();
    }
}

async function Zadanie11() {
    try {

        await titleCollection.updateOne(
            { primaryTitle: "Blade Runner", startYear: 1982 },
            { $pull: { rating: { averageRating: 10, numVotes: 55555 } } }
        );

        const BladeRunner1982AfterUpdate = await titleCollection.findOne({ primaryTitle: "Blade Runner", startYear: 1982 });
        console.log(BladeRunner1982AfterUpdate);

    } finally {
        await client.close();
    }
}

async function Zadanie12() {
    try {


        await titleCollection.updateOne(
            { primaryTitle: "Pan Tadeusz", startYear: 1999 },
            { $set: { avgRating: 9.1 } },
            { upsert: true }
        );

        const MrTadeusz = await titleCollection.findOne(
            { primaryTitle: "Pan Tadeusz", startYear: 1999 }
        );
        console.log(MrTadeusz)

    } finally {
        await client.close();
    }
}

async function Zadanie13() {
    try {

const moviesBeforeDelete= await titleCollection.countDocuments({startYear: {$lt:1989} })
console.log("Before count: "+moviesBeforeDelete)

const result = await titleCollection.deleteMany(
    { startYear: {$lt:1989} }
);

const moviesAfterDelete= await titleCollection.countDocuments({startYear: {$lt:1989} })
console.log("After count: "+moviesAfterDelete)
console.log(`${result.deletedCount} movies  was deleted`);

    } finally {
        await client.close();
    }
}
// Zadanie1();
// Zadanie2()
// Zadanie3()
// Zadanie4()
// Zadanie5()
// Zadanie6()
// Zadanie7()
// Zadanie8()
// Zadanie9()
// Zadanie10()
// Zadanie11()
// Zadanie12()
// Zadanie13()





