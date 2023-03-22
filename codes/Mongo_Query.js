db.February.aggregate(
    [
         { $project: 
         { "document.body": 1, "document.score": 1, "document.author": 1, "document.link_id": 1, "_id": 0 } },
          { $out: "filtered_feb" }
    ]
)