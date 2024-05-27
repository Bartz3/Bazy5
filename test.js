const { MongoClient } = require('mongodb');

const url = 'mongodb://127.0.0.1:27017'; // Użyj 127.0.0.1 zamiast localhost
const dbName = 'IMDB';

async function queryDatabase() {
  const client = new MongoClient(url);

  try {
    await client.connect();
    console.log('Połączono z MongoDB');

    const db = client.db(dbName);
    const titleCollection = db.collection('Title');
    const ratingCollection = db.collection('Rating');

    const pipeline = [
      {
        $match: {
          startYear: { $in: [2010, 2011, 2012] },
          genres: { $regex: 'Documentary', $options: 'i' }
        }
      },
      {
        $lookup: {
          from: 'Rating',
          localField: 'tconst',
          foreignField: 'tconst',
          as: 'ratings'
        }
      },
      {
        $unwind: '$ratings'
      },
      {
        $group: {
          _id: {
            tconst: '$tconst',
            title: '$primaryTitle',
            startYear: '$startYear'
          },
          averageRating: { $avg: '$ratings.averageRating' }
        }
      },
      {
        $sort: { averageRating: -1 }
      },
      {
        $project: {
          _id: 0,
          title: '$_id.title',
          year: '$_id.startYear',
          averageRating: 1
        }
      },
      {
        $limit: 5
      }
    ];

    const aggregationCursor = titleCollection.aggregate(pipeline);

    const results = await aggregationCursor.toArray();

    console.log('5 najlepszych filmów dokumentalnych (2010-2012) wg średniej oceny:');
    results.forEach((doc, index) => {
      console.log(`${index + 1}. Tytuł: ${doc.title}, Rok: ${doc.year}, Średnia ocena: ${doc.averageRating}`);
    });

    // Aby policzyć wszystkie dokumenty spełniające kryteria, usuń limit i zlicz wyniki
    const countPipeline = [...pipeline];
    countPipeline.pop(); // usuń limit
    countPipeline.push({
      $count: 'total'
    });

    const countCursor = titleCollection.aggregate(countPipeline);
    const countResults = await countCursor.toArray();

    console.log('Liczba dokumentów spełniających kryteria:', countResults[0]?.total || 0);

  } catch (err) {
    console.error('Błąd:', err);
  } finally {
    await client.close();
    console.log('Połączenie z MongoDB zostało zamknięte');
  }
}

queryDatabase();
