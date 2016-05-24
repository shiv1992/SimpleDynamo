package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	public static Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider/myTable");

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		Button ins=(Button)findViewById(R.id.button);
		Button que=(Button)findViewById(R.id.button2);
		final SimpleDynamoProvider sm=new SimpleDynamoProvider();

		ins.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {

				ContentValues Cv=new ContentValues();
				Cv.put("key","KEY1");
				Cv.put("value","VAL1");

				sm.insert(uri,Cv);
			}
		});

		que.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {

				try{
					Cursor cur= sm.query(uri,null,"KEY1",null,null);
					int i= cur.getCount();
					Log.v("Activity Count :",String.valueOf(i));
				}
				catch(Exception e){
					e.printStackTrace();
//					Log.d("Error",e)
				}

			}
		});





	}


	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
