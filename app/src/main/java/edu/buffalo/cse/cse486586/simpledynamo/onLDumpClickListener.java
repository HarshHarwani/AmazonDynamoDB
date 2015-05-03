package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

import java.util.Arrays;
/**
 * Created by hharwani on 4/26/15.
 */
    public class onLDumpClickListener implements View.OnClickListener {

        private static final String TAG = "onLDumpClickListener";
        private static final int TEST_CNT = 1;
        private static final String KEY_FIELD = "key";
        private static final String VALUE_FIELD = "value";

        private final TextView textView;
        private final ContentResolver mContentResolver;
        private final Uri mUri;

        public onLDumpClickListener(TextView _tv, ContentResolver _cr) {
            textView = _tv;
            mContentResolver = _cr;
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        public void onClick(View v) {

            String[] keyValues = testQuery();
            if (keyValues == null) {
                textView.append("Query fail\n");
            }

            textView.append(Arrays.toString(keyValues));

        }

        private String[] testQuery() {
            try {
                String selection = "@";
                Cursor resultCursor = mContentResolver.query(mUri, null, selection, null, null);
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }

                String keyValues[] = new String[resultCursor.getCount()];
                int i = 0;

                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

                resultCursor.moveToFirst();
                while (resultCursor.isAfterLast() == false) {
                    keyValues[i] = resultCursor.getString(keyIndex) +"--"+ resultCursor.getString(valueIndex);
                    i++;
                    resultCursor.moveToNext();
                }
                resultCursor.close();
                return keyValues;

            } catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        }

    }
