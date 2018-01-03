function contains(a, obj) {
        var i = a.length;
        while (i--) {
          if (a[i] === obj) {
            return true;
          }
        }
        return false;
      }
function display_papers(papers, ordered, num_to_display){

        if (ordered)
          document.write('<ol>')

        for (var y in entities)
        {

          if ((papers.length != 0 && !(contains(papers, y))) || (num_to_display == 0))
            continue;
          num_to_display--;

          if (ordered)
            document.write('<li>');
          if (entities[y]["location"] == null)
            document.write('<span class="label label-primary">PRE-PRINT</span> ');
          else
            document.write('<span class="label label-primary">PAPER</span> ');
          document.write('<a href='+entities[y]["link"]+">"+entities[y]["title"]+"</a>. <br/>");
          document.write(entities[y]["authors"]+". ");
          if (entities[y]["location"] == null)
            document.write(entities[y]["type"]+ ". "+ entities[y]["date"]+"<br/>");
          else
            document.write('<b>'+entities[y]["type"]+ "</b>, "+ entities[y]["location"] + ". " + entities[y]["date"]+"<br/>");
          if (entities[y]["special"] != null)
            document.write('<span class="badge">'+entities[y]["special"]+'</span><br/>');
          if (ordered)
            document.write('</li>')
        }

        if (ordered)
          document.write('</ol>')

      }

function display_papers_year(papers, ordered, num_to_display){

        if (ordered)
          document.write('<ol>')

        old_year = "1000";

        for (var y in entities)
        {

          if ((papers.length != 0 && !(contains(papers, y))) || (num_to_display == 0))
            continue;
          num_to_display--;

          // new year-based-segment

          full_date = entities[y]["date"];
          year = full_date.substr(full_date.length - 4);
          if (year != old_year)
            {
              old_year = year;
              document.write('</ol>');
              document.write('<h2>'+year+'</h2>');
              document.write('<ol>');
            }

          // end new year-based-segment

          if (ordered)
            document.write('<li>');
          if (entities[y]["location"] == null)
            document.write('<span class="label label-primary">PRE-PRINT</span> ');
          else
            document.write('<span class="label label-primary">PAPER</span> ');
          document.write('<a href='+entities[y]["link"]+">"+entities[y]["title"]+"</a>. <br/>");
          document.write(entities[y]["authors"]+". ");
          if (entities[y]["location"] == null)
            document.write(entities[y]["type"]+ ". "+ entities[y]["date"]+"<br/>");
          else
            document.write('<b>'+entities[y]["type"]+ "</b>, "+ entities[y]["location"] + ". " + entities[y]["date"]+"<br/>");
          if (entities[y]["special"] != null)
            document.write('<span class="badge">'+entities[y]["special"]+'</span><br/>');
          if (ordered)
            document.write('</li>')
        }

        if (ordered)
          document.write('</ol>')

      }