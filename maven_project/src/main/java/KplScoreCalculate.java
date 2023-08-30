import lombok.Data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringJoiner;

/**
 * @author epochwang@tencent.com
 * @date 2022-07-18 17:29
 * @describe
 */
public class KplScoreCalculate {

    @Data
    static class Player {
        Integer id;
        String name;
        String gameName;
        Double score = 100.0;
        Boolean isMvp = false;
        /**
         * 0:为竞猜
         */
        int quizWinMvpPlayerId;
        int quizFailMvpPlayerId;
        Boolean quizRedIsWin;
        double giftScore = 0;

        public Player(String name) {
            this.name = name;
        }

        public Player(String name, Double score) {
            this.name = name;
            this.score = score;
        }

        public Player(String name, String gameName) {
            this.name = name;
            this.gameName = gameName;
        }

        public void printName() {
            System.out.println("编号: " + id + "\t" + " 昵称: " + name);
        }

        public void printScore() {
            System.out.println("昵称: " + name + " 分数:" + score);
        }
    }

    @Data
    static class Game {
        static MysqlConnect mysqlConnect = new MysqlConnect();
        static Scanner scanner = new Scanner(System.in);
        static ArrayList<Player> allPlayers = new ArrayList<Player>() {{
            add(new Player("夏日限定昭君"));
            add(new Player("冰霜之翼"));
            add(new Player("李激动桑素"));
            add(new Player("彼岸之芸"));
            add(new Player("额...册呢"));
            add(new Player("躺赢王者啊啊"));
            add(new Player("明明知多"));
            add(new Player("还没上尐学"));
            add(new Player("TU-TU-TU"));
            add(new Player("君子好玩儿"));
            add(new Player("疯狂de瑞文"));
            add(new Player("索尼A7R3 想要升级"));
            add(new Player("全@ada@eric"));
            add(new Player("贺恩力"));
            add(new Player("真预言家·康"));
            add(new Player("明明"));
            add(new Player("鸣"));
            add(new Player("清净至俭"));
            add(new Player("晚风吹行舟"));
            add(new Player("殊心"));
            add(new Player("謨硒"));
            add(new Player("你先这么叫吧"));
            add(new Player("秣皙"));
            add(new Player("艾樂"));
            add(new Player("牛油果ぃ"));
            add(new Player("假温柔。"));
            add(new Player("怪泡泡灵"));
            add(new Player("小宝"));
            add(new Player("虎年限定小九"));
            add(new Player("吴侠"));
            add(new Player("秋霜盎然"));
            add(new Player("Kim"));
            add(new Player("epochong"));
            add(new Player("王昊"));
            add(new Player("许多"));
            add(new Player("Chanson"));
            add(new Player("丽米丝"));
            add(new Player("伊莱雯巴"));
        }};

        static {
            for (int i = 0; i < allPlayers.size(); i++) {
                allPlayers.get(i).setId(i + 1);
            }
        }

        ArrayList<Player> redTeam = new ArrayList<>();
        ArrayList<Player> blueTeam = new ArrayList<>();
        /**
         * 未参加比赛的选手可参与比赛竞猜，包括胜负和mvp选手，猜中胜负可获得20积分，猜中mvp选手获得50分，猜错不扣分。
         */
        ArrayList<Player> quizTeam = new ArrayList<>();

        Map<String, Integer> gift = new HashMap<>();

        Boolean isRedWin;

        /**
         * 赛季
         */
        int season;

        /**
         * 场次
         */
        int screening;

        public void setGift() {
            System.out.println("赠予模式");
        }

        public void showPlayers(ArrayList<Player> players) {
            int i = 0;
            for (Player allPlayer : players) {
                if (i++ % 5 == 0) {
                    System.out.print("\n");
                } else {
                }
                allPlayer.printName();
            }
            System.out.println();
        }

        public Player switchPlayer() {
            showPlayers(allPlayers);
            System.out.print("选择竞猜的人编号:");
            int id;
            while (true) {
                id = scanner.nextInt();
                if (id >= allPlayers.size() || id < 0) {
                    System.out.println("没有该选手编号,输入错误重新输入!");
                } else {
                    break;
                }
            }
            return allPlayers.get(id - 1);
        }

        public void playGame() {
            System.out.println("输入比赛结果:");
            System.out.print("当前赛季(1,2,3...):>");
            this.season = scanner.nextInt();
            System.out.print("当前场次(1,2,3...):>");
            this.screening = scanner.nextInt();
            System.out.print("胜方(1: 红 2:蓝) 选择:>");
            int input = scanner.nextInt();
            int select = input;
            System.out.print("输入红方MVP选手编号:");
            input = scanner.nextInt();
            for (Player player : redTeam) {
                if (player.getId() == input) {
                    player.setIsMvp(true);
                }
            }
            System.out.print("输入蓝方MVP选手编号:");
            input = scanner.nextInt();
            for (Player player : blueTeam) {
                if (player.getId() == input) {
                    player.setIsMvp(true);
                }
            }
            if (select == 1) {
                isRedWin = true;
                redIsWin();
            }
            if (select == 2) {
                isRedWin = false;
                blueIsWin();
            }
        }

        public void setTeam() {
            System.out.print("选择红方玩家编号,空号间隔输入:>");
            int i = 0;
            while (i++ < 5) {
                int id = scanner.nextInt();
                if (id > allPlayers.size() || id < 1) {
                    i = 0;
                    redTeam.clear();
                    System.out.println("没有该选手编号,输入错误重新输入!");
                    System.out.print("选择红方玩家编号,空号间隔输入:>");
                    continue;
                }
                redTeam.add(allPlayers.get(id - 1));
            }

            System.out.print("选择蓝方玩家编号,空号间隔输入:>");
            scanner.nextLine();
            i = 0;
            while (i++ < 5) {
                int id = scanner.nextInt();
                if (id > allPlayers.size() || id < 1) {
                    i = 0;
                    blueTeam.clear();
                    System.out.println("没有该选手编号,输入错误重新输入!");
                    System.out.print("选择红方玩家编号,空号间隔输入:");
                    continue;
                }
                blueTeam.add(allPlayers.get(id - 1));
            }
            System.out.println("红方队伍如下:");
            showPlayers(redTeam);
            System.out.println("蓝方队伍如下:");
            showPlayers(blueTeam);
        }

        public void setQuiz() {
            Player player = switchPlayer();
            System.out.print("竞猜选择是(0:未竞猜1:红胜 2:蓝胜):>");
            int isRead;
            while (true) {
                isRead = scanner.nextInt();
                if (isRead < 1 || isRead > 2) {
                    printErrorInput();
                } else {
                    break;
                }
            }
            switch (isRead) {
                case 1:
                    player.setQuizRedIsWin(true);
                    break;
                case 2:
                    player.setQuizRedIsWin(false);
                    break;
                default:
                    player.setQuizRedIsWin(null);
                    break;

            }
            showPlayers(redTeam);
            System.out.print("竞猜红方MVP选手(没有竞猜输入0):>");
            int redMvpId = scanner.nextInt();
            player.setQuizWinMvpPlayerId(redMvpId);
            showPlayers(blueTeam);
            System.out.print("竞猜蓝方MVP选手(没有竞猜选择0):>");
            int blueMvpId = scanner.nextInt();
            player.setQuizFailMvpPlayerId(blueMvpId);
            quizTeam.add(player);
        }

        public void printErrorInput() {
            System.out.println("输入错误,重新输入。");

        }

        public void afterGame() {
            // 竞猜结果
            for (Player player : quizTeam) {
                // 竞猜MVP选手
                for (int i = 0; i < redTeam.size(); i++) {
                    if (redTeam.get(i).getId().equals(player.getQuizWinMvpPlayerId())) {
                        player.setScore(player.getScore() + 50);
                    }
                    if (blueTeam.get(i).getId().equals(player.getQuizFailMvpPlayerId())) {
                        player.setScore(player.getScore() + 50);
                    }

                }
                // 竞猜胜利方
                if (player.getQuizRedIsWin() != null && isRedWin.equals(player.getQuizRedIsWin())) {
                    player.setScore(player.getScore() + 20);
                }
            }
            // 赛季结束后，每名选手可将自身积分的10%赠予最认可的其他选手。
            for (Player player : redTeam) {
                int giftScore = gift.get(player.getName()) == null ? 0 : gift.get(player.getName());
                player.setScore(player.getScore() + giftScore);
            }
            for (Player player : blueTeam) {
                int giftScore = gift.get(player.getName()) == null ? 0 : gift.get(player.getName());
                player.setScore(player.getScore() + giftScore);
            }
            saveGameResult();
        }

        public void printGameResult() {
            System.out.println("胜利队伍: " + (isRedWin ? "红方!" : "蓝方!"));
            System.out.println("选手得分如下:");
            System.out.println("红方:");
            for (Player player : redTeam) {
                player.printScore();
            }
            System.out.println("蓝方:");
            for (Player player : blueTeam) {
                player.printScore();
            }
        }

        public void saveGameResult() {
            Connection connection = mysqlConnect.connect();
            String format = "(%d,'%s',%d,%d,%.3f,%d,%d,%d,%d)";
            PreparedStatement statement;
            StringJoiner joiner = new StringJoiner(",");

            for (Player player : redTeam) {
                joiner.add(String.format(format, player.getId(), player.getName(), this.getSeason(),
                        this.getScreening(),
                        player.getScore(), player.getIsMvp() ? 1 : 0, player.getQuizWinMvpPlayerId(),
                        player.getQuizFailMvpPlayerId(), player.getQuizRedIsWin() == null ? 0 : 1));
            }
            for (Player player : blueTeam) {
                joiner.add(String.format(format, player.getId(), player.getName(), this.getSeason(),
                        this.getScreening(),
                        player.getScore(), player.getIsMvp() ? 1 : 0, player.getQuizWinMvpPlayerId(),
                        player.getQuizFailMvpPlayerId(), player.getQuizRedIsWin() == null ? 0 : 1));
            }
            for (Player player : quizTeam) {
                joiner.add(String.format(format, player.getId(), player.getName(), this.getSeason(),
                        this.getScreening(),
                        player.getScore(), player.getIsMvp() ? 1 : 0, player.getQuizWinMvpPlayerId(),
                        player.getQuizFailMvpPlayerId(), player.getQuizRedIsWin() == null ? 0 : 1));
            }

            String sql =
                    "INSERT INTO kplscorecalculate (player_id, player_name, season,screening,score,is_mvp," +
                            "quiz_win_mvp_player_id,quiz_fail_mvp_player_id,quiz_red_is_win) " +
                            "VALUES " + joiner +
                            " ON DUPLICATE KEY UPDATE player_id=VALUES(player_id),season=VALUES(season)," +
                            "screening=VALUES(screening);";
            System.out.println("slq:" + sql);
            try {
                statement = connection.prepareStatement(sql);
                statement.executeUpdate();
            } catch (Exception e) {
                System.err.println("error:" + e);
            } finally {
                mysqlConnect.close();
            }
        }

        public void getSeasonResult() {
            Connection connection = mysqlConnect.connect();
            PreparedStatement statement;
            System.out.print("查询赛季(1,2,3...):>");
            int querySeason = scanner.nextInt();

            String sql =
                    "select player_name, sum(score) as all_score from kplscorecalculate where score > 0 and " +
                            "season =" + querySeason + " group " +
                            "by player_id,player_name order by all_score desc;";
            try {
                statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery();
                int i = 0;
                while (resultSet.next()) {
                    Player player = new Player(resultSet.getString("player_name"), resultSet.getDouble("all_score"));
                    player.printScore();
                    i++;
                }
                if (i == 0) {
                    System.out.println("当前赛季没有比赛记录!");
                }
            } catch (Exception e) {
                System.err.println("initDailyStatus64 error:" + e);
            } finally {
                mysqlConnect.close();
            }
        }


        public void redIsWin() {
            // 失败方每人扣除自身积分的30%（单次积分损失不超过100）作为获胜方奖励
            double allFailScore = 0;
            for (Player player : blueTeam) {
                double failScore = player.getScore() * 0.3 > 100 ? 100 : player.getScore() * 0.3;
                allFailScore += failScore;
                player.setScore(player.getScore() - failScore);
            }
            // 其中获胜方mvp获得30%积分，剩余积分由其他选手评分
            double mvpWinScore = allFailScore * 0.3;
            double otherWinScore = (allFailScore - mvpWinScore) / 4;
            for (Player player : redTeam) {
                if (player.getIsMvp()) {
                    player.setScore(player.getScore() + mvpWinScore);
                } else {
                    player.setScore(player.getScore() + otherWinScore);
                }
            }
            for (Player player : blueTeam) {
                if (player.getIsMvp()) {
                    player.setScore(player.getScore() * 1.3);
                }
            }
        }

        public void blueIsWin() {
            // 失败方每人扣除自身积分的30%（单次积分损失不超过100）作为获胜方奖励
            double allFailScore = 0;
            for (Player player : redTeam) {
                double failScore = player.getScore() * 0.3 > 100 ? 100 : player.getScore() * 0.3;
                allFailScore += failScore;
                player.setScore(player.getScore() - failScore);
            }
            // 其中获胜方mvp获得30%积分，剩余积分由其他选手评分
            double mvpWinScore = allFailScore * 0.3;
            double otherWinScore = (allFailScore - mvpWinScore) / 4;
            for (Player player : blueTeam) {
                if (player.getIsMvp()) {
                    player.setScore(player.getScore() + mvpWinScore);
                } else {
                    player.setScore(player.getScore() + otherWinScore);
                }
            }
            for (Player player : redTeam) {
                if (player.getIsMvp()) {
                    player.setScore(player.getScore() * 1.3);
                }
            }
        }


        public void startGame() {
            // 每场比赛开赛前每名参赛选手获得50分
            for (Player player : redTeam) {
                player.setScore(player.getScore() + 50);
            }
            for (Player player : blueTeam) {
                player.setScore(player.getScore() + 50);
            }
        }

    }

    static Scanner scanner = new Scanner(System.in);

    /**
     * 第三届昭君杯将采用积分模式。积分的来源包括选手参加竞技比赛获得场次分和获胜分。非参赛选手参与竞猜、志愿者积分奖励。具体规则如下：
     * 1、[玫瑰]参加比赛，人人拿分[玫瑰]每名具有准入资格（准入资格以主办方口径为准）的选手初始积分为100分，每场比赛开赛前每名参赛选手获得50分；
     * 2、[玫瑰]你们的积分，都拿来吧[玫瑰]每场比赛结束后，失败方每人扣除自身积分的30%（单次积分损失不超过100）作为获胜方奖励，其中获胜方mvp获得30%积分，剩余积分由其他选手平分。
     * 3、[玫瑰]谁是大预言家[玫瑰]未参加比赛的选手可参与比赛竞猜，包括胜负和mvp选手，猜中胜负可获得20积分，猜中mvp选手获得50分，猜错不扣分。
     * 4、[玫瑰]我是勤劳小蜜蜂[玫瑰]参与比赛组织、分数统计的志愿者，组委会将按照贡献值，给与50-200的积分奖励。
     * 6、[玫瑰]我的队伍我做主[玫瑰]每次报名不进行预先分组，选手可自由组队[旺柴]
     * 5、[玫瑰]友谊地久天长[玫瑰]赛季结束后，每名选手可将自身积分的10%赠予最认可的其他选手。
     * 7、[玫瑰]金主爸爸说了算[玫瑰]最终解释权归主办方所有
     * 8、[玫瑰]你就是英雄[玫瑰]每次统计日积分前三的选手可获得将一名英雄送入荣誉殿堂的机会（英雄由其他选手投票产生），被送入荣誉殿堂的英雄该选手在本赛季无法使用
     *
     * @param args
     */
    public static void main(String[] args) {
        Game game = new Game();
        while (true) {
            printMenu();

            int select = scanner.nextInt();
            switch (select) {
                case 1:
                    game.showPlayers(Game.allPlayers);
                    game.setTeam();
                    break;
                case 2:
                    game.startGame();
                    game.playGame();
                    break;
                case 3:
                    game.setQuiz();
                    break;
                case 4:
                    game.afterGame();
                    game.printGameResult();
                    break;
                case 5:
                    game.getSeasonResult();
                    break;
                case 6:
                    game = new Game();
                    break;
                default:
                    System.out.println("请重新输入!");
            }
            System.out.println();
        }
    }


    public static void printMenu() {
        System.out.println("输入数字进行选择");
        System.out.println("1.输入比赛队伍信息");
        System.out.println("2.输入比赛结果");
        System.out.println("3.设置竞猜");
        System.out.println("4.保存比赛结果");
        System.out.println("5.统计赛季结果");
        System.out.println("6.数据清空");
        System.out.print("您的选择是: ");
    }

    static class MysqlConnect {
        private static final String DATABASE_DRIVER = "com.mysql.jdbc.Driver";
        /**
         * 需要设置一下useSSL，设置为false来禁用SSL，或者设置为true来使用SSL，报这个警告的原因主要是JDBC的版本与MySQL的版本不兼容，而MySQL在高版本需要指明是否进行SSL连接。
         */
        private static final String DATABASE_URL = "jdbc:mysql://localhost:3306/game?useUnicode=true" +
                "&characterEncoding=utf8&useSSL=false";
        private static final String USERNAME = "root";
        private static final String PASSWORD = "wangchong";
        private static final String MAX_POOL = "200";

        private Connection connection;
        private Properties properties;

        private Properties getProperties() {
            if (properties == null) {
                properties = new Properties();
                properties.setProperty("user", USERNAME);
                properties.setProperty("password", PASSWORD);
                properties.setProperty("MaxPooledStatements", MAX_POOL);
            }
            return properties;
        }

        public Connection connect() {
            if (connection == null) {
                try {
                    Class.forName(DATABASE_DRIVER);
                    connection = DriverManager.getConnection(DATABASE_URL, getProperties());
                } catch (ClassNotFoundException | SQLException e) {
                }
            }
            return connection;
        }

        public void close() {
            if (connection != null) {
                try {
                    connection.close();
                    connection = null;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
