package cn.com.wiseweb.gather;

/**
 * Created by yangguihu on 2016/10/30.
 */
public class KafkaHbaseUtils {

    /**
     * 根据site_id 解析出group_id
     * @param site_id   site_id
     * @param url       url地址,根据url区分blog
     * @param groupid   传递的默认的group_id
     * @return           通过site_id解析
     */
    public static String siteGroupMapping(Object site_id, Object url, String groupid){
        try{
            //返回group_id  默认为传入的 默认值
            String group_id= groupid;
            int siteid = Integer.valueOf(site_id.toString());

            //新闻
            if((1<=siteid && siteid <=59999) || (70000<=siteid && siteid <=89999)||(100000<=siteid && siteid <200000)){
                if(url.toString().contains("blog"))
                    group_id= "3";   //包含blog 为播客
                else
                    group_id= "1";  //新闻
            }
            //论坛
            if(60000<=siteid && siteid <=69999){
                group_id= "2";
            }
            //外媒 (水利部中文没有负值)
            if(90000<=siteid && siteid <=99999){
                group_id= "7";
            }
            //纸媒
            if(siteid>=200000){
                if(siteid==200598 || siteid==200599){
                    group_id= "1"; //新闻
                }else if(siteid==200611|| siteid==200621|| siteid==200622||siteid==200628){
                    group_id= "3"; //播客
                }else if(siteid == 200620){
                    group_id = "14";  //问答
                }if((200623<=siteid && siteid <=200627)||(200629<=siteid && siteid <=200635)){   //新闻客户端
                    group_id= "13";
                }if(30001<=siteid && siteid<=300065){
                    group_id= "1"; //首页新闻
                }else{
                    group_id="5";  //纸媒
                }
            }

            //负值的时候
            //=======微信================
            //银行微信
            if(url.toString().contains("http://mp.weixin.qq.com")){return "11";}

//            if((siteid>= -5242 &&siteid <=-5238)||siteid==-4999||siteid==-7651){return "11";}
//            //银行微信公众号
//            if(siteid>= -7972 &&siteid <=-7762){return "11";}
//            //食药微信
//            if((-11712 <= siteid&&siteid <=-11618)||siteid==-11714||siteid==-14880){return "11";}
//            //食药微信公众号
//            if(-14974 <= siteid&&siteid <=-14881){return "11";}
//            //中国兵团微信
//            if(-16255 <= siteid&&siteid <=-16171){return "11";}
//            //央视微信
//            if(-20445 <= siteid&&siteid <=-20439){return "11";}
//            //央视微信公众号
//            if(-21570 <= siteid&&siteid <=-21386){return "11";}
//            //日照银行微信
//            if(-72231 <= siteid&&siteid <=-72174){return "11";}
//            //影视微信
//            if(-87948 <= siteid&&siteid <=-85475){return "11";}
//            //影视微信公众号
//            if(-88554 <= siteid&&siteid <=-88546){return "11";}
//            //新加微信
//            if(-3602 <= siteid&&siteid <=-3598){return "11";}

            //外媒（系统已有）
            if((-90293 <= siteid&&siteid <=-90000)||(-90360 <= siteid&&siteid <=-90334)||(-90400 <= siteid&&siteid <=-90370)){return "7";}
            //水利部中文 (没有负值site_id)
            //水利部俄语
            if(-90368 <= siteid&&siteid <=-90361){return "7";}

            return group_id;

        }catch (Exception e){
            System.out.println("site_id 解析group_id失败");
            return groupid;
        }
    }

    public static void main(String[] args) {
        String s = siteGroupMapping("-14881", "http://blog.sina.com.cn/s/blog_53d2ac990102wwmq.html?tj=1", "1");
        System.out.println(s);
    }

}
