;
(function($) {
    var app = $.sammy(function() {
        $(function() {
            /* $("#datepicker").datepicker({
                onSelect_off: function() {
                    var id = $("#username").html();
                    var date, sql;
                    date = new Date($("#datepicker").val()).toISOString().split("T")[0]
                    sql = `"select * from HEALTHKIT.HK.POP_AGG where id ='` + id + `' and date='` + date + `' and (protein>=0 or carbs>=0 or fattotal>=0) order by date desc;"`
                    $.ajax({
                        "async": false,
                        "crossDomain": true,
                        "url": "https://jkduwmfkfb.execute-api.us-east-1.amazonaws.com/dev/send",
                        "method": "POST",
                        data: `{"body":{"select":` + sql + `}}`,
                        success: function(result) {
                            refresh_data(result.body);
                        }
                    });
                }
            });
            */
        });
        this.get('#/pin/:id', function() {
            var id = this.params['id'];
            $("#username").html(id+" - ");
            $("#pininput").val(id);
            picked_date=new Date();
            $("#selected_date").html(picked_date.toDateString());
            get_data();
        });
        this.get('#/pin/:id/date/:date', function() {
            var id = this.params['id'];
            $("#username").html(id+" - ");
            $("#pininput").val(id);
            picked_date=new Date(this.params['date']+"T12:00:00.978Z");
            $("#selected_date").html(picked_date.toDateString());
            get_data();
        });


    });

    $(function() {
        app.run()
    });
})(jQuery);