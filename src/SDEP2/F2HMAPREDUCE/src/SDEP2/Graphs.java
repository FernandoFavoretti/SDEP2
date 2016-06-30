package SDEP2;

import java.awt.Color;
import java.awt.Font;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPosition;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class Graphs extends ApplicationFrame {

    public Graphs(String applicationTitle, String chartTitle, String serie, String data, String diretorioPart) throws IOException {

        super(applicationTitle);
        JFreeChart lineChart = ChartFactory.createLineChart(
                chartTitle,
                serie, data,
                createDataset(diretorioPart, chartTitle),
                PlotOrientation.VERTICAL,
                true, true, false);

        int width = 3000;
        int height = 600;
        File bigChartFile = new File(diretorioPart + "/bigChart.jpeg");
        ChartUtilities.saveChartAsJPEG(bigChartFile, lineChart, width, height);
        int width2 = 1000;

        int height2 = 500;
        File lineChartFile = new File(diretorioPart + "/normalChart.jpeg");
        ChartUtilities.saveChartAsJPEG(lineChartFile, lineChart, width2, height2);

        ChartPanel chartPanel = new ChartPanel(lineChart);
        chartPanel.setPreferredSize(new java.awt.Dimension(1000, 400));
        setContentPane(chartPanel);
    }

    private DefaultCategoryDataset createDataset(String diretorioPart, String serie) throws IOException {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        String file = "part-r-00000";

        try (BufferedReader br = new BufferedReader(new FileReader(diretorioPart + "/" + file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("	");
                dataset.addValue(Double.parseDouble(parts[1]), serie, parts[0]);
            }
        }

        return dataset;
    }

}
