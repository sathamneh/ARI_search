package com.arexperts;

public class Article {
    String articleText;
    String articleID;

    public static Article build(String text, String id) {
        Article newArticle =  new Article();

        newArticle.articleID = id;
        newArticle.articleText = text;

        return newArticle;
    }
}

