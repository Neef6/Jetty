# Spark
1.alorithm
    简单实现 pageRank
2.spark advanced data analysis
    scala数据分析相关基础
3.streaming

4.spark——mllib
    #决策树算法预测森林植被
    构建决策树：同一份数据分成3份，0.8－trans 0.1－cross_test 0.1－test cv评估训练的参数
    7:input_data每个类别特征值预期的不同取值个数，所以生成的是7＊7的矩阵，每一行对应正确类别，每一列对应预测值。及第i行j列代表一个正确类别是i的样本被预测为类别j的次数。
    准确度：预测正确占总预测的比率
    精度分析：精确度，召回率。对准确度的评估。
    随机准确度：每类在训练集喝cv集合出现的概率相乘，结果相加。随机的和之前
    不纯性度量：gini
    随机决策森林：
5.mllib 课后作业
    数据信息：用户安装列表，数据格式：上报日期、用户id、安装包名


