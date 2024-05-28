const { MongoClient } = require('mongodb');

const url = 'mongodb://127.0.0.1:27017'; // Użyj 127.0.0.1 zamiast localhost
const dbName = 'IMDB';

async function findTopDocumentaries() {
  const client = new MongoClient(url);

  try {
    await client.connect();
    console.log('Połączono z MongoDB');

    const db = client.db(dbName);
    const titleCollection = db.collection('Title');

    const cursor = titleCollection.aggregate([
      { 
        $match: { 
          startYear: { $in: [2010, 2011, 2012] }, 
          genres: { $regex: "Documentary" } 
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
          avgRating: "$joinRating.averageRating" 
        }
      },
      { 
        $sort: { avgRating: -1 } 
      },
      { 
        $limit: 5 
      }
    ]);

    let count = 0;
    for await (const doc of cursor) {
      console.log(doc);
      count++;
      if (count >= 5) {
        break;  // Zatrzymuje iterację po znalezieniu 5 rekordów
      }
    }

  } catch (err) {
    console.error('Błąd:', err);
  } finally {
    await client.close();
    console.log('Połączenie z MongoDB zostało zamknięte');
  }
}

findTopDocumentaries();
