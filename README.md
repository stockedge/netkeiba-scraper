#使い方
以下のコマンドを上から順に実行していけば最後に素性が作成される。
```
sbt "run collecturl"
```
レース結果が載っているURLを収集して「race_list.txt」に保存する。
```
sbt "run scrapehtml"
```
レース結果のHTMLをスクレイピングしてhtmlフォルダに保存する。HTMLをまるごとスクレイピングするので結構時間がかかる。
```
sbt "run extract"
```
HTMLからレース結果を抜き出しSQLiteに保存する。
```
sbt "run genfeature"
```
レース結果を元にして素性を作りSQLiteに保存する。