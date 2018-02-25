
function update() {
    $.ajax({
        url: '/log-update',
        success:  function(data) {
            $('#log').text(data.content);
            update();
        },
        timeout: 500000 //If timeout is reached run again
    });
}

/**
 * Perform first data request. After taking this data, just query the
 * server and refresh when answered (via update call).
 */
function load() {
    $.ajax({
        url: '/log',
        success: function(data) {
            $('#log').text(data.content);
            update();
        }
    });
}


$(function (){

    load();    
    var $status = $('#status');
    var stateValue;

    $.ajax({
        type: 'GET',
        url: 'api/ctews/status',
        success: function(state){
            $status.text('Status: ' + state.status + ' Tweets Collected: ' + state.tweets);
            stateValue = state.status;
        },
        error: function() {
            alert('Error loading status');
            console.log("No State");
            stateValue = "No State";
        }
    });

    $('#toggleState').on('click', function(){
        console.log(stateValue);
        var newState;
        if(stateValue){
            newState = "disconnect";
        }else if(stateValue == false){
            newState = "stream";
        }else{
            alert('State not loaded')
            newState('random')
        }
        console.log(newState);
        $.ajax({
            type: 'POST',
            url: 'api/ctews/status',
            data: JSON.stringify({status : newState}),
            contentType: "application/json",
            dataType: 'json',
            success: function(state){
                $status.text('Status: ' + state.status + ' Tweets Collected: ' + state.tweets);
                stateValue = state.status;
            },
            error: function() {
                alert('Error sending status');
                console.log(JSON.stringify({status: newState}));
            }
        });

    });
});
