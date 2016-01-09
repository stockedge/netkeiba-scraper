library(randomForest)
library(RSQLite)
 
randomRows <- function(df, n) {
  df[sample(nrow(df),n),]
}
 
downSample <- function(df) {
  c1 <- df[df$order_of_finish == "TRUE",]
  c2 <- df[df$order_of_finish == "FALSE",]
  size <- min(nrow(c1), nrow(c2))
  rbind(randomRows(c1,size), randomRows(c2,size))
}


drv <- dbDriver('SQLite')
conn <- dbConnect(drv, dbname='race.db')
rs <- dbSendQuery(conn, '
select 
  order_of_finish,
  race_id,
  horse_number,
  grade,
  age,
  avgsr4,
  avgWin4,
  dhweight,
  disRoc,
  r.distance,
  dsl,
  enterTimes,
  eps,
  hweight,
  jwinper,
  odds,
  owinper,
  preSRa,
  sex,
  f.surface,
  surfaceScore,
  twinper,
  f.weather,
  weight,
  winRun,
  jEps,
  jAvgWin4,
  preOOF,
  pre2OOF,
  month,
  runningStyle,
  preLastPhase,
  lateStartPer,
  course,
  placeCode,
  race_number
from 
  feature f 
inner join 
  race_info r 
on 
  f.race_id = r.id
where
  order_of_finish is not null
and 
  preSRa is not null
limit 250000')
 
allData <- fetch(rs, n = -1)

dbClearResult(rs)
dbDisconnect(conn)

#カテゴリ変数をファクターに変換しておく
allData$placeCode <- factor(allData$placeCode)
allData$month     <- factor(allData$month)
allData$grade     <- factor(allData$grade)
allData$sex       <- factor(allData$sex)
allData$weather   <- factor(allData$weather)
allData$surface   <- factor(allData$surface)
allData$course    <- factor(allData$course)

#負担重量/馬体重を素性に追加
allData$weightper <- allData$weight / allData$hweight

#オッズを支持率に変換
allData$support <- 0.788 / (allData$odds - 0.1)
allData$odds <- NULL

#着順をカテゴリ変数に変換
allData$order_of_finish <- factor(allData$order_of_finish == 1)

allData.s <- downSample(na.omit(allData))
allData.s <- allData.s[order(allData.s$race_id),]

#データを学習用とテスト用に分割する
train <- allData.s[1:(nrow(allData.s)-5000),]
test  <- allData.s[(nrow(allData.s)-4999):nrow(allData.s),]

#予測モデルを作成
(rf.model1 <- randomForest(
  order_of_finish ~ . - support - race_id, train))

#素性の重要度を見てみる
importance(rf.model1)

#テストデータで予測力を見てみる
pred <- predict(rf.model1, test)
tbl <- table(pred, test$order_of_finish)
sum(diag(tbl)) / sum(tbl)

#支持率だけを用いて予測モデルを作成する
(rf.model2 <- randomForest(
  order_of_finish ~ support, train))

pred <- predict(rf.model2, test)
tbl <- table(pred, test$order_of_finish)
sum(diag(tbl)) / sum(tbl)


racewiseFeature <- 
c("avgsr4",
"avgWin4",
"dhweight",
"disRoc",
"dsl",
"enterTimes",
"eps",
"hweight",
"jwinper",
"owinper",
"preSRa",
"twinper",
"weight",
"jEps",
"jAvgWin4",
"preOOF",
"pre2OOF",
"runningStyle",
"preLastPhase",
"lateStartPer",
"weightper",
"winRun")

splited.allData <- split(allData, allData$race_id)

scaled.allData <- unsplit(
  lapply(splited.allData,
    function(rw) {
      data.frame(
        order_of_finish = rw$order_of_finish,
        race_id = rw$race_id,
        age = rw$age,
        grade = rw$grade,
        distance = rw$distance,
        sex = rw$sex,
        weather = rw$weather,
        course = rw$course,
        month = rw$month,
        surface = rw$surface,
        surfaceScore = rw$surfaceScore,
        horse_number = rw$horse_number,
        placeCode = rw$placeCode,
        race_number = rw$race_number,
        support = rw$support,
        scale(rw[,racewiseFeature]))
    }),
  allData$race_id)

scaled.allData$order_of_finish = factor(scaled.allData$order_of_finish)

is.nan.df <- function(x) do.call(cbind, lapply(x, is.nan))
scaled.allData[is.nan.df(scaled.allData)] <- 0

scaled.allData <- downSample(na.omit(scaled.allData))
scaled.allData <- scaled.allData[order(scaled.allData$race_id),]

#データを学習用とテスト用に分割する
scaled.train <- scaled.allData[1:(nrow(scaled.allData)-5000),]
scaled.test  <- scaled.allData[(nrow(scaled.allData)-4999):nrow(scaled.allData),]

#レース毎に正規化されたデータで予測モデルを作成
(rf.model3 <- randomForest(
  order_of_finish ~ . - support - race_id, scaled.train))

#素性の重要度を見てみる
importance(rf.model3)

#テストデータで予測力を見てみる
pred <- predict(rf.model3, scaled.test)
tbl <- table(pred, scaled.test$order_of_finish)
sum(diag(tbl)) / sum(tbl)



#支持率を追加して予測モデルを作成
(rf.model4 <- randomForest(
  order_of_finish ~ support, train))

#素性の重要度を見てみる
importance(rf.model4)

#テストデータで予測力を見てみる
pred <- predict(rf.model4, test)
tbl <- table(pred, test$order_of_finish)
sum(diag(tbl)) / sum(tbl)


#支持率を追加して予測モデルを作成
(rf.model5 <- randomForest(
  order_of_finish ~ support, scaled.train))

#素性の重要度を見てみる
importance(rf.model5)

#テストデータで予測力を見てみる
pred <- predict(rf.model5, scaled.test)
tbl <- table(pred, scaled.test$order_of_finish)
sum(diag(tbl)) / sum(tbl)

