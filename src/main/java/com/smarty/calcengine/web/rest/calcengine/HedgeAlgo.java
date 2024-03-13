package com.smarty.calcengine.web.rest.calcengine;

import static com.smarty.calcengine.web.rest.calcengine.HedgeAlgoColumns.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.commons.math3.stat.StatUtils;
import tech.tablesaw.aggregate.AggregateFunction;
import tech.tablesaw.aggregate.NumericAggregateFunction;
import tech.tablesaw.api.*;
import tech.tablesaw.joining.DataFrameJoiner;
import tech.tablesaw.selection.Selection;

public class HedgeAlgo {

    private static final String BUY = "Buy";
    private static final String SELL = "Sell";
    private static final String MV_METRIC = "MV";
    private static final String CTD_METRIC = "CTD";
    private static final String LEFT_QTY = "LeftQty";
    private static final String ALLOC_PERC = "AllocPerc";
    private static final String ALLOC_MVAL = "AllocMVal";
    private static final String ALLOC_CTD = "AllocCTDur";
    private static final String MV_PROP_RANK = "MvPropRank";
    public static final String ALLOC_MV_PROP_RANK = "AllocMvPropRank";
    public static final String CTD_PROP_RANK = "CtdPropRank";
    public static final String ALLOC_CTD_PROP_RANK = "AllocCtdPropRank";
    public static final String RANK_READY_RANK = "RankReady";
    public static final String EXEC_READY_RANK = "ExecReady";
    public static final Double THRESHOLD = 0.01;
    private static final String PROP_VAL = "PropVal";
    private static final String NEG_QTY_PROP = "NegQtyProp";
    private static final String PROP_QTY = "PropQty";

    public Table buildOrderData() {
        String[] sides = { BUY, BUY, BUY, BUY, SELL, SELL, SELL, SELL, SELL };
        Integer[] orderIds = { 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009 };
        Integer[] ranks = { 1, 1, 2, 2, 3, 3, 4, 4, 5 };
        String[] metrics = { MV_METRIC, MV_METRIC, MV_METRIC, MV_METRIC, MV_METRIC, MV_METRIC, MV_METRIC, MV_METRIC, CTD_METRIC };
        Double[] quantities = { 1000.0d, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 500.0 };
        Double[] allocQuantities = { 100.0d, 0.0, 0.0, 0.0, 400.0, 200.0, 0.0, 0.0, 75.0 };
        Double[] marketValues = { 10000.0d, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 0.0 };
        Double[] CTDs = { 11.0, 11.0, 11.0, 11.0, 10.0, 10.0, 10.0, 10.0, 4.0 };

        StringColumn side_col = StringColumn.create(SIDE, sides);
        IntColumn orderId_col = IntColumn.create(ORDER_ID, orderIds);
        IntColumn rank_col = IntColumn.create(RANK, ranks);
        StringColumn metric_col = StringColumn.create(METRIC, metrics);
        DoubleColumn qty_col = DoubleColumn.create(QUANTITY, quantities);
        DoubleColumn allocQty_col = DoubleColumn.create(ALLOCATION_QTY, allocQuantities);
        DoubleColumn mkt_col = DoubleColumn.create(MARKET_VALUE, marketValues);
        DoubleColumn ctd_col = DoubleColumn.create(CTD, CTDs);

        Table orderData = Table
            .create(ORDER_DATA)
            .addColumns(side_col, orderId_col, rank_col, metric_col, qty_col, allocQty_col, mkt_col, ctd_col);

        return orderData;
    }

    private static Table enrichTable(Table orderData) {
        //derived columns
        DoubleColumn qty_col = (DoubleColumn) orderData.column(QUANTITY);
        DoubleColumn allocQty_col = (DoubleColumn) orderData.column(ALLOCATION_QTY);
        DoubleColumn mkt_col = (DoubleColumn) orderData.column(MARKET_VALUE);
        DoubleColumn ctd_col = (DoubleColumn) orderData.column(CTD);

        DoubleColumn leftQty_col = qty_col.subtract(allocQty_col).setName(LEFT_QTY);
        DoubleColumn allocPerc_col = allocQty_col.divide(qty_col).setName(ALLOC_PERC);
        DoubleColumn allocMVal_col = allocPerc_col.multiply(mkt_col).setName(ALLOC_MVAL);
        DoubleColumn allocCTDur_col = allocPerc_col.multiply(ctd_col).setName(ALLOC_CTD);

        orderData.addColumns(leftQty_col);
        orderData.addColumns(allocPerc_col);
        orderData.addColumns(allocMVal_col);
        orderData.addColumns(allocCTDur_col);

        return orderData;
    }

    private static Table addDerivedColumn(
        Table orderData,
        String newColName,
        String sideByCol,
        String rank_sideByCol,
        String side,
        String rank
    ) {
        NumericAggregateFunction custom_sum_1 = new NumericAggregateFunction("SUM_1") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                NumericColumn<?> numericColumn = (NumericColumn<?>) column.removeMissing();
                return StatUtils.sum(numericColumn.asDoubleArray());
            }
        };
        NumericAggregateFunction custom_sum_2 = new NumericAggregateFunction("SUM_2") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                NumericColumn<?> numericColumn = (NumericColumn<?>) column.removeMissing();
                return StatUtils.sum(numericColumn.asDoubleArray());
            }
        };
        Table MV_BY_RANK_SIDE = orderData.summarize(rank_sideByCol, custom_sum_1).groupBy(rank, side).apply().setName("TABLE1");
        Table MV_BY_SIDE = orderData.summarize(sideByCol, custom_sum_2).groupBy(side).apply().setName("TABLE2");

        DataFrameJoiner dataFrameJoiner2 = new DataFrameJoiner(MV_BY_RANK_SIDE, side);
        Table t3 = dataFrameJoiner2.inner(MV_BY_SIDE);
        //System.out.println(t3);

        DoubleColumn c1 = (DoubleColumn) t3.column("SUM_1 " + "[" + rank_sideByCol + "]");
        DoubleColumn c2 = (DoubleColumn) t3.column("Sum_2 " + "[" + sideByCol + "]");
        //System.out.println(c1.asList());
        //System.out.println(c2.asList());

        DoubleColumn c3 = DoubleColumn.create(newColName, c1.divide(c2).asDoubleArray());

        t3.addColumns(c3);
        t3.removeColumns("SUM_1 " + "[" + rank_sideByCol + "]", "SUM_2 " + "[" + sideByCol + "]");
        //System.out.println(t3);

        DataFrameJoiner dataFrameJoiner3 = new DataFrameJoiner(orderData, RANK, SIDE);
        orderData = dataFrameJoiner3.inner(t3);
        return orderData;
    }

    private List<Integer> exec_rank_fn(
        Double minRank,
        Double buyCtdRankMin,
        Double sellCtdRankMin,
        Double buyMvRankMin,
        Double sellMvRankMin,
        Double allocMvPercentBuy,
        Double allocMvPercentSell,
        Double allocCtdPercentBuy,
        Double allocCtdPercentSell,
        Double buyRankMin,
        Double sellRankMin,
        Double threshold
    ) {
        List<Integer> exec_rank = new ArrayList<>();

        if ((allocMvPercentBuy - allocMvPercentSell) > threshold && (allocCtdPercentBuy - allocCtdPercentSell) > threshold) {
            exec_rank.add(sellRankMin.intValue());
        } else if ((allocMvPercentSell - allocMvPercentBuy) > threshold && (allocCtdPercentSell - allocCtdPercentBuy) > threshold) {
            exec_rank.add(buyRankMin.intValue());
        } else if (Math.abs(allocMvPercentBuy - allocMvPercentSell) < threshold && (allocCtdPercentSell - allocCtdPercentBuy) > threshold) {
            exec_rank.add(buyCtdRankMin.intValue());
        } else if (Math.abs(allocMvPercentBuy - allocMvPercentSell) < threshold && (allocCtdPercentBuy - allocCtdPercentSell) > threshold) {
            exec_rank.add(sellCtdRankMin.intValue());
        } else {
            exec_rank.add(minRank.intValue());
        }

        return exec_rank;
    }

    private Table process(Table orderData) {
        System.out.println(orderData);

        orderData = addDerivedColumn(orderData, MV_PROP_RANK, MARKET_VALUE, MARKET_VALUE, SIDE, RANK);
        orderData = addDerivedColumn(orderData, ALLOC_MV_PROP_RANK, MARKET_VALUE, ALLOC_MVAL, SIDE, RANK);
        orderData = addDerivedColumn(orderData, CTD_PROP_RANK, CTD, CTD, SIDE, RANK);
        orderData = addDerivedColumn(orderData, ALLOC_CTD_PROP_RANK, CTD, ALLOC_CTD, SIDE, RANK);

        AggregateFunction<?, ?> sum = new NumericAggregateFunction("sum") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                NumericColumn<?> numericColumn = (NumericColumn<?>) column.removeMissing();
                return StatUtils.sum(numericColumn.asDoubleArray());
            }
        };

        AggregateFunction<?, ?> min = new NumericAggregateFunction("min") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                NumericColumn<?> numericColumn = (NumericColumn<?>) column.removeMissing();
                return StatUtils.min(numericColumn.asDoubleArray());
            }
        };

        Selection SIDE_SELL = orderData.stringColumn(SIDE).isEqualTo(SELL);
        Selection SIDE_BUY = orderData.stringColumn(SIDE).isEqualTo(BUY);
        Selection LEFT_QTY_GRT_ZERO = orderData.doubleColumn(LEFT_QTY).isGreaterThan(0.0);
        Selection METRIC_CTD = orderData.stringColumn(METRIC).isEqualTo(MV_METRIC);
        Selection METRIC_MV = orderData.stringColumn(METRIC).isEqualTo(CTD_METRIC);

        Double totMvBuy = orderData.where(SIDE_BUY).summarize(MARKET_VALUE, sum).apply().row(0).getDouble(0);
        Double totMvSell = orderData.where(SIDE_SELL).summarize(MARKET_VALUE, sum).apply().row(0).getDouble(0);
        Double totCtdBuy = orderData.where(SIDE_BUY).summarize(CTD, sum).apply().row(0).getDouble(0);
        Double totCtdSell = orderData.where(SIDE_SELL).summarize(CTD, sum).apply().row(0).getDouble(0);

        Double allocTotMvBuy = orderData.where(SIDE_BUY).summarize(ALLOC_MVAL, sum).apply().row(0).getDouble(0);
        Double allocTotMvSell = orderData.where(SIDE_SELL).summarize(ALLOC_MVAL, sum).apply().row(0).getDouble(0);
        Double allocTotCtdBuy = orderData.where(SIDE_BUY).summarize(ALLOC_CTD, sum).apply().row(0).getDouble(0);
        Double allocTotCtdSell = orderData.where(SIDE_SELL).summarize(ALLOC_CTD, sum).apply().row(0).getDouble(0);

        Double minRank = orderData.where(LEFT_QTY_GRT_ZERO).summarize(RANK, min).apply().row(0).getDouble(0);
        Double buyRankMin = orderData.where(LEFT_QTY_GRT_ZERO.and(SIDE_BUY)).summarize(RANK, min).apply().row(0).getDouble(0);
        Double sellRankMin = orderData.where(LEFT_QTY_GRT_ZERO.and(SIDE_SELL)).summarize(RANK, min).apply().row(0).getDouble(0);
        Double buyCtdRankMin = orderData
            .where(LEFT_QTY_GRT_ZERO.and(SIDE_BUY).and(METRIC_CTD))
            .summarize(RANK, min)
            .apply()
            .row(0)
            .getDouble(0);
        Double sellCtdRankMin = orderData
            .where(LEFT_QTY_GRT_ZERO.and(SIDE_SELL).and(METRIC_CTD))
            .summarize(RANK, min)
            .apply()
            .row(0)
            .getDouble(0);
        Double buyMvRankMin = orderData
            .where(LEFT_QTY_GRT_ZERO.and(SIDE_BUY).and(METRIC_MV))
            .summarize(RANK, min)
            .apply()
            .row(0)
            .getDouble(0);
        Double sellMvRankMin = orderData
            .where(LEFT_QTY_GRT_ZERO.and(SIDE_SELL).and(METRIC_MV))
            .summarize(RANK, min)
            .apply()
            .row(0)
            .getDouble(0);

        Double allocMvPercentBuy = allocTotMvBuy / totMvBuy;
        Double allocMvPercentSell = allocTotMvSell / totMvSell;
        Double allocCtdPercentBuy = allocTotCtdBuy / totCtdBuy;
        Double allocCtdPercentSell = allocTotCtdSell / totCtdSell;
        Double absMvPercDiff = Math.abs(allocMvPercentBuy - allocMvPercentSell);
        Double absCtdPercDiff = Math.abs(allocCtdPercentBuy - allocCtdPercentSell);

        List<Integer> execReadyRanksList = exec_rank_fn(
            minRank,
            buyCtdRankMin,
            sellCtdRankMin,
            buyMvRankMin,
            sellMvRankMin,
            allocMvPercentBuy,
            allocMvPercentSell,
            allocCtdPercentBuy,
            allocCtdPercentSell,
            buyRankMin,
            sellRankMin,
            THRESHOLD
        );

        List<Integer> minRankList = new ArrayList();
        minRankList.add(minRank.intValue());

        orderData = addRankColumns(orderData, execReadyRanksList, minRankList);
        orderData = addPropVal(orderData, absMvPercDiff);
        orderData = addProQtyCol(orderData, sum, METRIC_CTD, METRIC_MV);

        System.out.println(orderData);

        return orderData;
    }

    public static void main(String[] args) {
        HedgeAlgo algo = new HedgeAlgo();
        Table orderData = algo.buildOrderData();
        Table enrichedTable = enrichTable(orderData);
        algo.process(enrichedTable);
    }

    public Table calculate(Table table) {
        Table enrichedTable = enrichTable(table);
        return process(enrichedTable);
    }

    private static Table addProQtyCol(Table orderData, AggregateFunction<?, ?> sum, Selection METRIC_CTD, Selection METRIC_MV) {
        Selection PROP_VAL_SEL = orderData.doubleColumn(PROP_VAL).isLessThan(0.0);
        Selection EXEC_READY_TRUE_SEL = orderData.booleanColumn(EXEC_READY_RANK).isTrue();
        Selection PROP_VAL_GRT_SEL = orderData.doubleColumn(PROP_VAL).isGreaterThanOrEqualTo(0);

        Double sumNegQtyMv = orderData
            .where(PROP_VAL_SEL.and(EXEC_READY_TRUE_SEL).and(METRIC_MV))
            .summarize(PROP_VAL, sum)
            .apply()
            .row(0)
            .getDouble(0);
        Double sumNegQtyCtd = orderData
            .where(PROP_VAL_SEL.and(EXEC_READY_TRUE_SEL).and(METRIC_CTD))
            .summarize(PROP_VAL, sum)
            .apply()
            .row(0)
            .getDouble(0);

        Double sumMvalMV = orderData
            .where(
                orderData
                    .doubleColumn(PROP_VAL)
                    .isGreaterThanOrEqualTo(0)
                    .and(orderData.booleanColumn(EXEC_READY_RANK).isTrue())
                    .and(orderData.stringColumn(METRIC).isEqualTo(MV_METRIC))
            )
            .summarize(MARKET_VALUE, sum)
            .apply()
            .row(0)
            .getDouble(0);

        Double sumMvalCTD = orderData
            .where(
                orderData
                    .doubleColumn(PROP_VAL)
                    .isGreaterThanOrEqualTo(0)
                    .and(orderData.booleanColumn(EXEC_READY_RANK).isTrue())
                    .and(orderData.stringColumn(METRIC).isEqualTo(CTD_METRIC))
            )
            .summarize(CTD, sum)
            .apply()
            .row(0)
            .getDouble(0);

        DoubleColumn negQtyPropCol = DoubleColumn.create(NEG_QTY_PROP);
        DoubleColumn propQtyCol = DoubleColumn.create(PROP_QTY);
        orderData.forEach(
            new Consumer<Row>() {
                @Override
                public void accept(Row row) {
                    Double negQtyProp = 0.0;
                    if (row.getString(METRIC).equalsIgnoreCase(MV_METRIC)) {
                        negQtyProp = row.getDouble(MARKET_VALUE) / sumMvalMV;
                    } else if (row.getString(METRIC).equalsIgnoreCase(CTD_METRIC)) {
                        negQtyProp = row.getDouble(CTD) / sumMvalCTD;
                    }

                    if (!row.getBoolean(EXEC_READY_RANK) || row.getDouble(PROP_VAL) < 0.0) {
                        negQtyProp = 0.0;
                    }
                    negQtyPropCol.append(negQtyProp);

                    if (row.getString(METRIC).equalsIgnoreCase(MV_METRIC) && row.getDouble(PROP_VAL) > 0.0) {
                        propQtyCol.append(row.getDouble(PROP_VAL) + (negQtyProp * sumNegQtyMv));
                    } else if (row.getString(METRIC).equalsIgnoreCase(CTD_METRIC) && row.getDouble(PROP_VAL) > 0.0) {
                        propQtyCol.append(row.getDouble(PROP_VAL) + (negQtyProp * sumNegQtyCtd));
                    } else {
                        propQtyCol.append(0.0);
                    }
                }
            }
        );
        orderData.addColumns(negQtyPropCol, propQtyCol);
        System.out.println(sumMvalCTD + "," + sumMvalMV);
        return orderData;
    }

    private static Table addPropVal(Table orderData, Double absMvPercDiff) {
        DoubleColumn propValCol = DoubleColumn.create(PROP_VAL);

        orderData.forEach(
            new Consumer<Row>() {
                @Override
                public void accept(Row row) {
                    Double propVal = 0.0;
                    if (row.getBoolean(EXEC_READY_RANK) && row.getString(METRIC).equalsIgnoreCase(MV_METRIC) && absMvPercDiff > THRESHOLD) {
                        propVal =
                            (((row.getDouble(ALLOC_MV_PROP_RANK) + absMvPercDiff) / row.getDouble(MV_PROP_RANK)) *
                                row.getDouble(QUANTITY)) -
                            row.getDouble(ALLOCATION_QTY);
                    } else if (
                        row.getBoolean(EXEC_READY_RANK) && row.getString(METRIC).equalsIgnoreCase(CTD_METRIC) && absMvPercDiff > THRESHOLD
                    ) {
                        propVal =
                            (((row.getDouble(ALLOC_MV_PROP_RANK) + absMvPercDiff) / row.getDouble(CTD_PROP_RANK)) *
                                row.getDouble(QUANTITY)) -
                            row.getDouble(ALLOCATION_QTY);
                    }
                    propValCol.append(propVal);
                }
            }
        );
        orderData.addColumns(propValCol);
        return orderData;
    }

    private static Table addRankColumns(Table orderData, List<Integer> execReadyRanksList, List<Integer> minRankList) {
        IntColumn rankCol = orderData.intColumn(RANK);
        BooleanColumn rankReadyCol = BooleanColumn.create(RANK_READY_RANK);
        BooleanColumn execReadyCol = BooleanColumn.create(EXEC_READY_RANK);
        rankCol.forEach(
            new Consumer<Integer>() {
                @Override
                public void accept(Integer integer) {
                    if (minRankList.contains(integer)) {
                        rankReadyCol.append(true);
                    } else {
                        rankReadyCol.append(false);
                    }
                    if (execReadyRanksList.contains(integer)) {
                        execReadyCol.append(true);
                    } else {
                        execReadyCol.append(false);
                    }
                }
            }
        );
        orderData.addColumns(rankReadyCol, execReadyCol);
        return orderData;
    }
}
