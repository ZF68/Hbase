package cn.kgc;


import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;



public class JavaFxs extends Application {

    @Override
    public void start(Stage primaryStage) throws Exception {
        /**
         * 首先我们创建一个Button，当我们点击Button的时候，改变Button的内容。
         */

        final Button btnHello = new Button("Hello");
        /**
         * 设置btnHello按钮点击事件
         * 这里使用了Java8的Lambda表达式。setOnAction的参数为EventHandler<ActionEvent> value
         * EventHandler为一个接口，所以我们有三种方式实现EventHandler接口：
         * 1. 创建一个内部类
         * 2. 创建一个匿名类
         * 3. 使用Lambda表达式（适用于函数体不大的情况）
         */
        btnHello.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                btnHello.setText("Hello World, I am JavaFX!");
            }
        });

        /**
         *  BorderPane是一个用于布局的Pane，BoerderPane将面板分割为上下左右中五部分。
         *  我们可以将UI控件放置在BorderPane的上下左右和中间。
         *  这里将将Button放置在中间。
         */
        BorderPane pane = new BorderPane();
        pane.setCenter(btnHello);

        // 将pane加入到Scen中
        Scene scene = new Scene(pane, 500, 500);

        // 设置stage的scen，然后显示我们的stage
        primaryStage.setScene(scene);
        primaryStage.setTitle("Hello World");
        primaryStage.show();
    }

    public static void main(String[] args) {
        // JavaFX中main函数必须需要调用launch函数
        launch(args);
    }
}