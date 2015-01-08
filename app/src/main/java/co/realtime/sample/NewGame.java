package co.realtime.sample;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;


public class NewGame extends ActionBarActivity {

    Game game;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_new_game);
        SharedPreferences sp = getSharedPreferences("values", Context.MODE_PRIVATE);
        String nickname = sp.getString("nickname", "");

        game = new Game(this, nickname);

    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        game.close();
    }
}
