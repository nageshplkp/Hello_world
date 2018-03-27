$(function() {
    $('.multiselect').each(function() {
        var parent = $(this);
        var selectContainer = parent.find('>:first-child');
        var select = selectContainer.find('select');
        var options = parent.find('.options');
        var checkboxes = options.find('input[type=checkbox]')

        // Click event to cancel closing current drop down if child is clicked vs body
        parent.on('click', function(e) {
            e.stopPropagation();
            return $(this).find(e.target).length >= 0;
        });

        // Prevent the select drop down from showing
        select.on('focus mousedown', function(e) {
            if (e.stopPropagation) {
                e.stopPropagation();
                e.preventDefault();
            }
        });

        // Update the text of the select drop down
        var updateSelectText = function() {
            var checkedItems = checkboxes.filter(':checked');

            var text = '';
            switch (checkedItems.length) {
                case 1:
                    text = checkedItems[0].value;
                    break;
                case 0:
                    break;
                default:
                    text = checkedItems.length +' items selected';
            }

            select
                .find('option')
                .remove()
                .end()
                .append('<option>' + text + '</option>');
        };
        updateSelectText();

        // Add click event to the select container
        parent.toggleExpand = function(e) {
            if (options.hasClass('expanded')) {
                $('body').off('click', parent.toggleExpand);

                parent.removeClass('expanded');
                options.removeClass('expanded');
            } else {
                options.css({
                    top: select.outerHeight(),
                    width: select.outerWidth()
                });

                parent.addClass('expanded');
                options.addClass('expanded');

                $('body').on('click', parent.toggleExpand);
            };
        };
        selectContainer.on('click', parent.toggleExpand);

        // Add checked event to the select container
        checkboxes.on('change', updateSelectText);
    });
});
