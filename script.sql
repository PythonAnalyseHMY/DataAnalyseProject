-- 根据user_id, product_id, times, dates去重
SELECT user_id, product_id, times, dates FROM tbub GROUP BY user_id, product_id, times, dates HAVING COUNT(*) > 1;

-- 用户流量分析
-- 分析页面浏览量PV和独立访客量UV
CREATE TABLE df_pv_uv(
  dates VARCHAR(10),
  PV INT(9),
  UV INT(9),
  PVUV DECIMAL(9, 2)
);

INSERT INTO df_pv_uv
  SELECT
    dates,
    COUNT(IF(action_type = 'pv', 1, NULL)) AS PV,
    COUNT(DISTINCT user_id) AS UV,
    ROUND(COUNT(IF(action_type = 'pv', 1, NULL)) / COUNT(DISTINCT user_id), 2) AS PVUV
  FROM
    tbub
  GROUP BY
    dates;

-- 用户留存分析
-- 计算用户单日留存率
CREATE TABLE df_retention_1(dates CHAR(10), retention_1 FLOAT);

INSERT INTO df_retention_1
  SELECT
    ub1.dates,
    COUNT(ub2.user_id) / COUNT(ub1.user_id) AS rentention_1
  FROM
    (SELECT DISTINCT user_id, dates FROM tbub) ub1
    LEFT JOIN (SELECT DISTINCT user_id, dates FROM tbub) ub2 ON ub1.user_id = ub2.user_id
    AND ub2.dates = DATE_ADD(ub1.dates, INTERVAL 1 DAY)
  GROUP BY
    ub1.dates;

-- 计算用户三日留存率
CREATE TABLE df_retention_3(dates CHAR(10), retention_3 FLOAT);

INSERT INTO df_retention_3
  SELECT
    ub1.dates,
    COUNT(ub2.user_id) / COUNT(ub1.user_id) AS rentention_3
  FROM
    (SELECT DISTINCT user_id, dates FROM tbub) ub1
    LEFT JOIN (SELECT DISTINCT user_id, dates FROM tbub) ub2 ON ub1.user_id = ub2.user_id
    AND ub2.dates = DATE_ADD(ub1.dates, INTERVAL 3 DAY)
  GROUP BY
    ub1.dates;

-- 用户行为分析
-- 以时间序列的逻辑，汇总每天每个小时执行PV、CART、FAV和BUY的次数
CREATE TABLE df_timeseries(
  dates CHAR(10),
  hours INT(9),
  PV INT(9),
  CART INT(9),
  FAV INT(9),
  BUY INT(9)
);
INSERT INTO df_timeseries
  SELECT
    dates,
    hours,
    COUNT(IF(action_type = 'pv', 1, NULL)) AS PV,
    COUNT(IF(action_type = 'cart', 1, NULL)) AS CART,
    COUNT(IF(action_type = 'fav', 1, NULL)) AS FAV,
    COUNT(IF(action_type = 'buy', 1, NULL)) AS BUY
  FROM
    tbub
  GROUP BY
    dates, hours
  ORDER BY
    dates ASC, hours ASC;

-- 用户路径分析
-- 本项目仅研究有浏览行为的用户路径
CREATE TABLE path AS
-- 定义临时表ubt
WITH
  ubt AS(
    SELECT
      user_id,
      category_id,
      COUNT(IF(action_type = 'PV', 1, NULL)) AS PV,
      COUNT(IF(action_type = 'CART', 1, NULL)) AS CART,
      COUNT(IF(action_type = 'FAV', 1, NULL)) AS FAV,
      COUNT(IF(action_type = 'BUY', 1, NULL)) AS BUY
    FROM
      tbub
    GROUP BY
      user_id, category_id
  ),
-- 定义临时表ifubt,它可以引用ubt临时表
  ifubt AS(
    SELECT
      user_id,
      category_id,
      IF(PV > 0, 1, 0) AS IFPV,
      IF(CART > 0, 1, 0) AS IFCART,
      IF(FAV > 0, 1, 0) AS IFFAV,
      IF(BUY > 0, 1, 0) AS IFBUY
    FROM
      ubt
    GROUP BY
      user_id, category_id
  ),
-- 定义临时表user_path,它可以引用ubt和ifubt临时表
  user_path AS (SELECT user_id, category_id, CONCAT(IFPV, IFCART, IFFAV, IFBUY) AS path FROM ifubt)
-- 主查询开始
SELECT
  user_id,
  category_id,
  path,
  CASE
    WHEN path = 1101 THEN '浏览-收藏-/-购买'
    WHEN path = 1011 THEN '浏览-/-加购-购买'
    WHEN path = 1111 THEN '浏览-收藏-加购-购买'
    WHEN path = 1001 THEN '浏览-/-/-购买'
    WHEN path = 1010 THEN '浏览-/-加购-/'
    WHEN path = 1100 THEN '浏览-收藏-/-/'
    WHEN path = 1110 THEN '浏览-收藏-加购-/'
    ELSE '浏览-/-/-/'
  END AS buy_path
FROM
  user_path
WHERE
  path REGEXP '^1';

-- 漏斗模型
CREATE TABLE funnel AS
  SELECT
    dates,
    COUNT(DISTINCT CASE WHEN action_type = 'pv' THEN user_id END) AS PV_NUM,
    COUNT(DISTINCT CASE WHEN action_type = 'cart' THEN user_id END) + COUNT(DISTINCT CASE WHEN action_type = 'fav' THEN user_id END) AS FAV_NUM,
    COUNT(DISTINCT CASE WHEN action_type = 'buy' THEN user_id END) AS BUY_NUM
  FROM
    tbub
  GROUP BY
    dates;

-- 用户画像分析：RFC模型：最近一次消费时间（Recency）、消费频率（Frequency）、收藏&加购（Cart&Favorite）
CREATE TABLE df_rfc AS
WITH
  r AS(
    SELECT
      user_id,
      MAX(dates) AS recency
    FROM
      tbub
    WHERE
      action_type = 'buy'
    GROUP BY
      user_id
  ),
  f AS(
    SELECT
      user_id,
      COUNT(*) AS frequency
    FROM
      tbub
    WHERE
      action_type = 'buy'
    GROUP BY
      user_id
  ),
  c AS(
    SELECT
      user_id,
      COUNT(*) AS cart_fav_count
    FROM
      tbub
    WHERE
      action_type IN ('cart', 'fav')
    GROUP BY
      user_id
  ),
  rfc_base AS(
    SELECT
      r.user_id,
      r.recency,
      f.frequency,
      c.cart_fav_count
    FROM
      r
    LEFT JOIN f on f.user_id = r.user_id
    LEFT JOIN c on c.user_id = r.user_id
  ),
  rfc_score AS(
    SELECT
      user_id,
      recency,
      CASE
        WHEN recency = '2017-12-03' THEN 100
        WHEN recency IN ('2017-12-02', '2017-12-01') THEN 80
        WHEN recency IN ('2017-11-30', '2017-11-29') THEN 60
        WHEN recency IN ('2017-11-28', '2017-11-27') THEN 40
        ELSE 20
      END AS r_score,
      frequency,
      CASE 
        WHEN frequency > 15 THEN 100
        WHEN frequency BETWEEN 12 AND 14 THEN 90
        WHEN frequency BETWEEN 9 AND 11 THEN 70
        WHEN frequency BETWEEN 6 AND 8 THEN 50 
        WHEN frequency BETWEEN 3 AND 5 THEN 30
        ELSE 10 
      END AS f_score,
      cart_fav_count,
      CASE 
        WHEN cart_fav_count > 20 THEN 100
        WHEN cart_fav_count BETWEEN 16 AND 20 THEN 85
        WHEN cart_fav_count BETWEEN 11 AND 15 THEN 70
        WHEN cart_fav_count BETWEEN 6 AND 10 THEN 55 
        WHEN cart_fav_count BETWEEN 1 AND 5 THEN 40
        ELSE 20 
      END AS c_score
    FROM
      rfc_base
  )
SELECT
  t1.user_id,
  recency,
  r_score,
  avg_r,
  frequency,
  f_score,
  avg_f,
  cart_fav_count,
  c_score,
  avg_c,
  CASE
    WHEN (f_score >= avg_f AND r_score >= avg_r AND c_score >= avg_c) THEN '价值用户'
    WHEN (f_score >= avg_f AND r_score >= avg_r AND c_score < avg_c) THEN '潜力用户'
    WHEN (f_score >= avg_f AND r_score < avg_r AND c_score >= avg_c) THEN '活跃用户'
    WHEN (f_score >= avg_f AND r_score < avg_r AND c_score < avg_c) THEN '保持用户'
    WHEN (f_score < avg_f AND r_score >= avg_r AND c_score >= avg_c) THEN '发展用户'
    WHEN (f_score < avg_f AND r_score >= avg_r AND c_score < avg_c) THEN '新用户'
    WHEN (f_score < avg_f AND r_score < avg_r AND c_score >= avg_c) THEN '兴趣用户'
    ELSE '挽留用户'
  END AS user_class
FROM
  rfc_score t1
LEFT JOIN (
  SELECT
    user_id,
    AVG(r_score) OVER() AS avg_r,
    AVG(f_score) OVER() AS avg_f,
    AVG(c_score) OVER() AS avg_c
  FROM
    rfc_score
) t2 on t2.user_id = t1.user_id;

-- RFC模型用户计数
CREATE TABLE df_rfc_count AS
  SELECT
    user_class,
    COUNT(*) AS user_class_num
  FROM
    df_rfc
  GROUP BY
    user_class;

-- 热卖商品分析
-- 统计销量最多的TOP1000商品
CREATE TABLE product_buy_hot AS(
  SELECT
    product_id,
    COUNT(IF(action_type = 'buy', 1, NULL)) AS product_buy
  FROM
    tbub
  GROUP BY
    product_id
  ORDER BY
    product_buy DESC
  LIMIT 1000
);

-- 统计销量最多的TOP100商品品类
CREATE TABLE category_buy_hot AS(
  SELECT
    category_id,
    COUNT(IF(action_type = 'buy', 1, NULL)) AS category_buy
  FROM
    tbub
  GROUP BY
    category_id
  ORDER BY
    category_buy DESC
  LIMIT 100
);

-- 品类决策时长：计算用户在特定商品上第一次浏览时间到第一次购买的时间，这段时间称为决策时长
CREATE TABLE categpory_buy_pv_time AS
WITH
  bought_categories AS(
-- 筛选有购买行为的用户-品类对
    SELECT DISTINCT
      user_id,
      category_id
    FROM
      tbub
    WHERE
      action_type = 'buy'
  ),
  first_pv AS(
-- 计算每个用户-品类的首次浏览时间
    SELECT
      t.user_id,
      t.category_id,
      MIN(t.full_time) AS first_pv_time
    FROM
      tbub t
      JOIN bought_categories bc ON t.user_id = bc.user_id AND t.category_id = bc.category_id
    WHERE
      t.action_type = 'pv'
    GROUP BY
      t.user_id,
      t.category_id
  ),
  first_buy AS(
-- 计算每个用户-品类的首次购买时间
    SELECT
      t.user_id,
      t.category_id,
      MIN(t.full_time) AS first_buy_time
    FROM
      tbub t
      JOIN bought_categories bc ON t.user_id = bc.user_id AND t.category_id = bc.category_id
    WHERE
      t.action_type = 'buy'
    GROUP BY
      t.user_id,
      t.category_id
  ),
  user_category_conversion AS(
-- 计算每个用户-品类的转化时间
    SELECT
      p.user_id,
      p.category_id,
      p.first_pv_time,
      b.first_buy_time,
      TIMESTAMPDIFF(SECOND, p.first_pv_time, b.first_buy_time) AS conversion_seconds
    FROM
      first_pv p
      INNER JOIN first_buy b on p.user_id = b.user_id AND p.category_id = b.category_id AND p.first_pv_time < b.first_buy_time
  ),
category_avg_conversion AS(
-- 计算相同品类的平均转化时间
  SELECT
    category_id,
    AVG(conversion_seconds) AS avg_conversion_seconds,
    AVG(conversion_seconds) / 3600 AS avg_conversion_hours
  FROM
    user_category_conversion
  GROUP BY
    category_id
  )
-- 最终结果：关联原始记录和品类平均时间
SELECT
  ucc.user_id,
  ucc.category_id,
  ucc.first_pv_time,
  ucc.first_buy_time,
  ucc.conversion_seconds / 3600 AS conversion_hours,
  cat.avg_conversion_hours,
-- 与品类平均值的偏差
  (ucc.conversion_seconds / 3600) - cat.avg_conversion_hours AS hours_deviation
FROM
  user_category_conversion ucc
  JOIN category_avg_conversion cat on cat.category_id = ucc.category_id
WHERE
  ucc.first_pv_time > '2017-11-25'
ORDER BY
  ucc.category_id,
  ucc.user_id;

-- 品类流量分析
-- 分析每个品类每个小时的流量
CREATE TABLE  category_hours_flow
  SELECT
    category_id,
    hours,
    SUM(IF(action_type = 'pv', 1, 0)) AS pv,
    SUM(IF(action_type = 'cart', 1, 0)) AS cart,
    SUM(IF(action_type = 'fav', 1, 0)) AS fav,
    SUM(IF(action_type = 'buy', 1, 0)) AS buy
  FROM
    tbub
  GROUP BY
    category_id,
    hours
  ORDER BY
    category_id,
    hours;

-- 分析每个品类每天的流量
CREATE TABLE  category_daily_flow
  SELECT
    category_id,
    dates,
    SUM(IF(action_type = 'pv', 1, 0)) AS pv,
    SUM(IF(action_type = 'cart', 1, 0)) AS cart,
    SUM(IF(action_type = 'fav', 1, 0)) AS fav,
    SUM(IF(action_type = 'buy', 1, 0)) AS buy
  FROM
    tbub
  GROUP BY
    category_id,
    dates
  ORDER BY
    category_id,
    dates;

-- 品类特征分析：统计每个品类下用户行为的汇总
CREATE TABLE category_feature
  SELECT
    category_id,
    COUNT(IF(action_type = 'pv', 1, NULL)) AS pv,
    COUNT(IF(action_type = 'cart', 1, NULL)) AS cart,
    COUNT(IF(action_type = 'fav', 1, NULL)) AS fav,
    COUNT(IF(action_type = 'buy', 1, NULL)) AS buy
  FROM
    tbub
  GROUP BY
    category_id;