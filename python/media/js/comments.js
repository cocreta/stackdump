/**
 * Bits of JavaScript used on the question page in Stackdump,
 * (https://bitbucket.org/samuel.lai/stackdump/).
 *
 * Requires jQuery.
 */

function hideExcessCommentsInit() {
    // this function initialises the 'hiding excess comments' functionality.
    
    // show the show-comments links and attach a click event handler to them
    $('.show-comments').show();
    $('.show-comments a').click(showHiddenComments);
    
    // hide the all the hidden comments
    $('.hidden-comment').hide();
}

function showHiddenComments() {
    // once comments are show, they can't be hidden again
    
    // get the relevant hidden comments
    var comments = $(this).closest('.show-comments').siblings('ul').children('li.hidden-comment');
    
    // show the comments
    comments.show();
        
    // hide the link
    $(this).closest('.show-comments').hide();
    
    return false;
}

$(document).ready(hideExcessCommentsInit);